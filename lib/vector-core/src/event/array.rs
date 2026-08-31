#![deny(missing_docs)]
//! This module contains the definitions and wrapper types for handling
//! arrays of type `Event`, in the various forms they may appear.

use std::{iter, slice, vec};

use futures::{Stream, stream};
#[cfg(test)]
use quickcheck::{Arbitrary, Gen};
use vector_buffers::EventCount;
use vector_common::{
    byte_size_of::ByteSizeOf,
    finalization::{
        AddBatchNotifier, BatchNotifier, EventFinalizerGroups, EventFinalizers, Finalizable,
        GroupedFinalizable, MergeFinalizable,
    },
    json_size::JsonSize,
};

use super::{
    EstimatedJsonEncodedSizeOf, Event, EventDataEq, EventFinalizer, EventMetadata, EventMutRef,
    EventRef, LogEvent, Metric, TraceEvent,
};

/// The type alias for an array of `TraceEvent` elements.
pub type TraceArray = Vec<TraceEvent>;

/// The type alias for an array of `Metric` elements.
pub type MetricArray = Vec<Metric>;

/// A batch of `LogEvent`s.
///
/// Historically this was a bare `Vec<LogEvent>` (row-major, aliased `LogArray`).
/// It is now a nominal type that can hold that same row-major vector *or*, with
/// the `columnar` feature enabled, a column-major Arrow `RecordBatch`.
///
/// The columnar representation is a *bypass*, never a replacement: anything that
/// needs per-event row semantics goes through [`LogBatch::into_events`] (owned)
/// or [`LogBatch::as_rows_mut`] (in place), both of which materialize a columnar
/// batch back into `LogEvent`s. Correctness is therefore always preserved; only
/// components that opt into reading the columnar variant directly (see
/// [`LogBatch::repr`]) skip the materialization.
#[derive(Clone, Debug, PartialEq)]
pub struct LogBatch(LogRepr);

/// The physical representation backing a [`LogBatch`].
///
/// `#[non_exhaustive]` so that a future columnar format is an additive variant
/// (behind its own feature) rather than a breaking change to every out-of-crate
/// `match` site. Same-crate matches are still checked exhaustively.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum LogRepr {
    /// Row-major: the historical `Vec<LogEvent>` layout.
    Rows(Vec<LogEvent>),
    /// Column-major: an Arrow `RecordBatch` plus batch-level metadata.
    #[cfg(feature = "columnar")]
    Columns {
        /// The columnar payload. Its schema travels inside the batch.
        batch: arrow::record_batch::RecordBatch,
        /// Metadata for the batch, since there are no `LogEvent`s to carry it.
        meta: BatchMetadata,
    },
}

/// Metadata carried by a columnar [`LogBatch`].
///
/// A columnar batch has no per-event `LogEvent`s to hang `EventMetadata` on, so
/// the metadata rides alongside the payload instead.
#[cfg(feature = "columnar")]
#[derive(Clone, Debug, PartialEq)]
pub enum BatchMetadata {
    /// Firehose fast path: the whole batch came from one source frame, so a
    /// single finalizer/metadata set covers every row.
    Shared(EventMetadata),
    /// Row-parallel sidecar: `meta[i]` describes row `i` (e.g. after a coalesce
    /// merged two batches of differing provenance).
    PerRow(Vec<EventMetadata>),
}

impl LogBatch {
    /// Construct a row-major batch from a vector of [`LogEvent`]s.
    #[must_use]
    pub fn from_rows(rows: Vec<LogEvent>) -> Self {
        Self(LogRepr::Rows(rows))
    }

    /// Construct a column-major batch from an Arrow `RecordBatch` and its
    /// batch-level metadata.
    #[cfg(feature = "columnar")]
    #[must_use]
    pub fn columns(batch: arrow::record_batch::RecordBatch, meta: BatchMetadata) -> Self {
        Self(LogRepr::Columns { batch, meta })
    }

    /// Borrow the underlying representation, e.g. so a columnar-aware sink can
    /// consume the `RecordBatch` without materializing rows.
    #[must_use]
    pub fn repr(&self) -> &LogRepr {
        &self.0
    }

    /// Consume a columnar batch, returning its `RecordBatch` and batch metadata.
    ///
    /// Returns the batch unchanged as `Err` if it is row-backed, so a caller can
    /// fall back to the row path. Note that finalizers live in the returned
    /// [`BatchMetadata`]; drain them with [`LogBatch::take_finalizers`] first if
    /// they are needed separately.
    #[cfg(feature = "columnar")]
    pub fn into_record_batch(
        self,
    ) -> Result<(arrow::record_batch::RecordBatch, BatchMetadata), LogBatch> {
        match self.0 {
            LogRepr::Columns { batch, meta } => Ok((batch, meta)),
            other => Err(LogBatch(other)),
        }
    }

    /// The number of events (rows) in this batch. Cheap on both reprs.
    #[must_use]
    pub fn len(&self) -> usize {
        match &self.0 {
            LogRepr::Rows(v) => v.len(),
            #[cfg(feature = "columnar")]
            LogRepr::Columns { batch, .. } => batch.num_rows(),
        }
    }

    /// Is this batch empty?
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// A reference to the first event, if any.
    ///
    /// Cheap on `Rows`. Immutable row access has no `LogEvent` to borrow from a
    /// `Columns` batch, so this is only valid once the batch is row-major;
    /// columnar fast-path callers must use [`LogBatch::repr`] instead. Call
    /// [`LogBatch::as_rows_mut`] first if you hold a `&mut` and need to force
    /// materialization.
    #[must_use]
    pub fn first(&self) -> Option<EventRef<'_>> {
        match &self.0 {
            LogRepr::Rows(v) => v.first().map(EventRef::from),
            #[cfg(feature = "columnar")]
            LogRepr::Columns { .. } => {
                panic!("`LogBatch::first` on a columnar batch; use `repr()` or materialize first")
            }
        }
    }

    /// Materialize a columnar batch into rows *in place* and return mutable
    /// access to the underlying `Vec<LogEvent>`.
    ///
    /// Row batches pay nothing. Columnar batches pay the (correct) explode,
    /// reattaching [`BatchMetadata`] onto each `LogEvent`. Despite the `as_`
    /// name this can allocate — the row path is the free one.
    pub fn as_rows_mut(&mut self) -> &mut Vec<LogEvent> {
        #[cfg(feature = "columnar")]
        if matches!(self.0, LogRepr::Columns { .. }) {
            let rows = std::mem::replace(&mut self.0, LogRepr::Rows(Vec::new()));
            self.0 = LogRepr::Rows(rows.into_rows());
        }
        match &mut self.0 {
            LogRepr::Rows(v) => v,
            #[cfg(feature = "columnar")]
            LogRepr::Columns { .. } => unreachable!("materialized above"),
        }
    }

    /// Iterate over references to the events in this batch.
    ///
    /// Like [`LogBatch::first`], immutable iteration requires a row-major batch;
    /// columnar fast-path callers must use [`LogBatch::repr`]/[`LogBatch::into_events`].
    pub fn iter(&self) -> slice::Iter<'_, LogEvent> {
        match &self.0 {
            LogRepr::Rows(v) => v.iter(),
            #[cfg(feature = "columnar")]
            LogRepr::Columns { .. } => {
                panic!("`LogBatch::iter` on a columnar batch; use `repr()` or materialize first")
            }
        }
    }

    /// Iterate over mutable references to the events in this batch.
    ///
    /// Materializes a columnar batch in place first.
    pub fn iter_mut(&mut self) -> slice::IterMut<'_, LogEvent> {
        self.as_rows_mut().iter_mut()
    }

    /// Apply `f` to the metadata of every event in the batch.
    ///
    /// For a columnar batch this operates on the batch-level [`BatchMetadata`]
    /// **without materializing** — a `Shared` set is touched once (it covers
    /// every row), a `PerRow` sidecar per entry. This is what lets source-side
    /// metadata stamping keep the columnar fast path intact.
    pub fn for_each_metadata_mut(&mut self, mut f: impl FnMut(&mut EventMetadata)) {
        match &mut self.0 {
            LogRepr::Rows(v) => {
                for log in v {
                    f(log.metadata_mut());
                }
            }
            #[cfg(feature = "columnar")]
            LogRepr::Columns { meta, .. } => meta.for_each_metadata_mut(f),
        }
    }

    /// Attach a batch notifier to every event in the batch.
    pub fn add_batch_notifier(&mut self, batch: BatchNotifier) {
        match &mut self.0 {
            LogRepr::Rows(v) => v
                .iter_mut()
                .for_each(|item| item.add_finalizer(EventFinalizer::new(batch.clone()))),
            #[cfg(feature = "columnar")]
            LogRepr::Columns { meta, .. } => meta.add_batch_notifier(batch),
        }
    }

    /// Take the finalizers from every event in the batch.
    pub fn take_finalizers(&mut self) -> EventFinalizers {
        match &mut self.0 {
            LogRepr::Rows(v) => v.iter_mut().map(Finalizable::take_finalizers).collect(),
            #[cfg(feature = "columnar")]
            LogRepr::Columns { meta, .. } => meta.take_finalizers(),
        }
    }

    /// Append another batch onto this one, coalescing in place.
    ///
    /// Two columnar batches with identical schemas concatenate via
    /// `arrow::compute::concat_batches` and stay columnar (their metadata is
    /// merged by [`super::columnar::merge_metadata`]). Every other combination
    /// — row+row, mismatched schemas, or a mix of reprs — materializes to rows.
    pub fn merge(&mut self, other: LogBatch) {
        let left = std::mem::replace(&mut self.0, LogRepr::Rows(Vec::new()));
        // The final arm is unreachable when the `columnar` feature is off (only
        // `Rows` exists then), but is a real coalescing path when it is on.
        #[allow(unreachable_patterns)]
        match (left, other.0) {
            (LogRepr::Rows(mut a), LogRepr::Rows(b)) => {
                a.extend(b);
                self.0 = LogRepr::Rows(a);
            }
            #[cfg(feature = "columnar")]
            (LogRepr::Columns { batch: a, meta: am }, LogRepr::Columns { batch: b, meta: bm })
                if a.schema() == b.schema() =>
            {
                let (a_rows, b_rows) = (a.num_rows(), b.num_rows());
                let batch = arrow::compute::concat_batches(&a.schema(), [&a, &b])
                    .expect("identical schemas concatenate");
                let meta = super::columnar::merge_metadata(am, a_rows, bm, b_rows);
                self.0 = LogRepr::Columns { batch, meta };
            }
            (left, right) => {
                let mut rows = left.into_rows();
                rows.extend(right.into_rows());
                self.0 = LogRepr::Rows(rows);
            }
        }
    }

    /// Take one finalizer group per event.
    ///
    /// Grouped finalizers require a one-group-per-event layout (the merge side
    /// asserts the counts match), so a columnar batch is materialized to rows
    /// first.
    pub fn take_finalizer_groups(&mut self) -> EventFinalizerGroups {
        self.as_rows_mut()
            .iter_mut()
            .map(Finalizable::take_finalizers)
            .collect()
    }
}

impl Default for LogBatch {
    fn default() -> Self {
        Self(LogRepr::Rows(Vec::new()))
    }
}

impl From<Vec<LogEvent>> for LogBatch {
    fn from(rows: Vec<LogEvent>) -> Self {
        Self::from_rows(rows)
    }
}

impl FromIterator<LogEvent> for LogBatch {
    fn from_iter<I: IntoIterator<Item = LogEvent>>(iter: I) -> Self {
        Self::from_rows(iter.into_iter().collect())
    }
}

impl From<LogBatch> for Vec<LogEvent> {
    fn from(batch: LogBatch) -> Self {
        batch.0.into_rows()
    }
}

impl LogRepr {
    /// Materialize into a row-major `Vec<LogEvent>`, exploding a columnar batch
    /// if necessary. This is the universal fallback that guarantees correctness
    /// for every consumer that has not opted into the columnar representation.
    fn into_rows(self) -> Vec<LogEvent> {
        match self {
            LogRepr::Rows(v) => v,
            #[cfg(feature = "columnar")]
            LogRepr::Columns { batch, meta } => super::columnar::explode(&batch, meta),
        }
    }
}

impl ByteSizeOf for LogBatch {
    fn allocated_bytes(&self) -> usize {
        match &self.0 {
            LogRepr::Rows(v) => v.allocated_bytes(),
            #[cfg(feature = "columnar")]
            LogRepr::Columns { batch, .. } => batch.get_array_memory_size(),
        }
    }
}

impl EstimatedJsonEncodedSizeOf for LogBatch {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        match &self.0 {
            LogRepr::Rows(v) => v.estimated_json_encoded_size_of(),
            // A rough proxy: the in-memory columnar footprint. Only relevant to
            // consumers that keep the batch columnar, which do not currently use
            // this for encoding decisions.
            #[cfg(feature = "columnar")]
            LogRepr::Columns { batch, .. } => JsonSize::new(batch.get_array_memory_size()),
        }
    }
}

impl EventDataEq for LogBatch {
    fn event_data_eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (LogRepr::Rows(a), LogRepr::Rows(b)) => a.event_data_eq(b),
            #[cfg(feature = "columnar")]
            _ => Vec::<LogEvent>::from(self.clone()).event_data_eq(&Vec::from(other.clone())),
        }
    }
}

impl EventContainer for LogBatch {
    type IntoIter = iter::Map<vec::IntoIter<LogEvent>, fn(LogEvent) -> Event>;

    fn len(&self) -> usize {
        LogBatch::len(self)
    }

    fn into_events(self) -> Self::IntoIter {
        Vec::<LogEvent>::from(self).into_iter().map(Into::into)
    }
}

#[cfg(feature = "columnar")]
impl BatchMetadata {
    /// Apply `f` to each `EventMetadata` set carried by the batch.
    fn for_each_metadata_mut(&mut self, mut f: impl FnMut(&mut EventMetadata)) {
        match self {
            Self::Shared(m) => f(m),
            Self::PerRow(v) => {
                for m in v {
                    f(m);
                }
            }
        }
    }

    /// Attach a batch notifier to the batch metadata.
    fn add_batch_notifier(&mut self, batch: BatchNotifier) {
        match self {
            Self::Shared(m) => m.add_finalizer(EventFinalizer::new(batch)),
            Self::PerRow(v) => {
                for m in v {
                    m.add_finalizer(EventFinalizer::new(batch.clone()));
                }
            }
        }
    }

    /// Take the finalizers from the batch metadata.
    fn take_finalizers(&mut self) -> EventFinalizers {
        match self {
            Self::Shared(m) => m.take_finalizers(),
            Self::PerRow(v) => v.iter_mut().map(EventMetadata::take_finalizers).collect(),
        }
    }
}

/// The core trait to abstract over any type that may work as an array
/// of events. This is effectively the same as the standard
/// `IntoIterator<Item = Event>` implementations, but that would
/// conflict with the base implementation for the type aliases below.
pub trait EventContainer: ByteSizeOf + EstimatedJsonEncodedSizeOf {
    /// The type of `Iterator` used to turn this container into events.
    type IntoIter: Iterator<Item = Event>;

    /// The number of events in this container.
    fn len(&self) -> usize;

    /// Is this container empty?
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Turn this container into an iterator over `Event`.
    fn into_events(self) -> Self::IntoIter;
}

/// Turn a container into a futures stream over the contained `Event`
/// type.  This would ideally be implemented as a default method on
/// `trait EventContainer`, but the required feature (associated type
/// defaults) is still unstable.
/// See <https://github.com/rust-lang/rust/issues/29661>
pub fn into_event_stream(container: impl EventContainer) -> impl Stream<Item = Event> + Unpin {
    stream::iter(container.into_events())
}

impl EventContainer for Event {
    type IntoIter = iter::Once<Event>;

    fn len(&self) -> usize {
        1
    }

    fn is_empty(&self) -> bool {
        false
    }

    fn into_events(self) -> Self::IntoIter {
        iter::once(self)
    }
}

impl EventContainer for LogEvent {
    type IntoIter = iter::Once<Event>;

    fn len(&self) -> usize {
        1
    }

    fn is_empty(&self) -> bool {
        false
    }

    fn into_events(self) -> Self::IntoIter {
        iter::once(self.into())
    }
}

impl EventContainer for Metric {
    type IntoIter = iter::Once<Event>;

    fn len(&self) -> usize {
        1
    }

    fn is_empty(&self) -> bool {
        false
    }

    fn into_events(self) -> Self::IntoIter {
        iter::once(self.into())
    }
}

impl EventContainer for MetricArray {
    type IntoIter = iter::Map<vec::IntoIter<Metric>, fn(Metric) -> Event>;

    fn len(&self) -> usize {
        self.len()
    }

    fn into_events(self) -> Self::IntoIter {
        self.into_iter().map(Into::into)
    }
}

/// An array of one of the `Event` variants exclusively.
#[derive(Clone, Debug, PartialEq)]
pub enum EventArray {
    /// A batch of type `LogEvent` (row- or, with the `columnar` feature,
    /// column-backed; see [`LogBatch`]).
    Logs(LogBatch),
    /// An array of type `Metric`
    Metrics(MetricArray),
    /// An array of type `TraceEvent`
    Traces(TraceArray),
}

impl EventArray {
    /// Iterate over references to this array's events.
    pub fn iter_events(&self) -> impl Iterator<Item = EventRef<'_>> {
        match self {
            Self::Logs(array) => EventArrayIter::Logs(array.iter()),
            Self::Metrics(array) => EventArrayIter::Metrics(array.iter()),
            Self::Traces(array) => EventArrayIter::Traces(array.iter()),
        }
    }

    /// Iterate over mutable references to this array's events.
    pub fn iter_events_mut(&mut self) -> impl Iterator<Item = EventMutRef<'_>> {
        match self {
            Self::Logs(array) => EventArrayIterMut::Logs(array.iter_mut()),
            Self::Metrics(array) => EventArrayIterMut::Metrics(array.iter_mut()),
            Self::Traces(array) => EventArrayIterMut::Traces(array.iter_mut()),
        }
    }

    /// Iterate over references to the logs in this array.
    pub fn iter_logs_mut(&mut self) -> impl Iterator<Item = &mut LogEvent> {
        match self {
            Self::Logs(array) => TypedArrayIterMut(Some(array.iter_mut())),
            _ => TypedArrayIterMut(None),
        }
    }

    /// Force any columnar log batch into its row representation.
    ///
    /// A no-op for row-shaped batches (and for metrics/traces). Use this before
    /// an immutable per-event pass (e.g. [`EventArray::iter_events`]) that would
    /// otherwise not be expressible over a columnar batch.
    pub fn materialize(&mut self) {
        if let Self::Logs(logs) = self {
            let _ = logs.as_rows_mut();
        }
    }

    /// Applies a closure to each event's metadata in this array.
    pub fn for_each_metadata_mut(&mut self, mut f: impl FnMut(&mut EventMetadata)) {
        match self {
            Self::Logs(logs) => logs.for_each_metadata_mut(f),
            Self::Metrics(metrics) => {
                for metric in metrics {
                    f(metric.metadata_mut());
                }
            }
            Self::Traces(traces) => {
                for trace in traces {
                    f(trace.metadata_mut());
                }
            }
        }
    }
}

impl From<Event> for EventArray {
    fn from(event: Event) -> Self {
        match event {
            Event::Log(log) => Self::Logs(vec![log].into()),
            Event::Metric(metric) => Self::Metrics(vec![metric]),
            Event::Trace(trace) => Self::Traces(vec![trace]),
        }
    }
}

impl From<LogEvent> for EventArray {
    fn from(log: LogEvent) -> Self {
        Event::from(log).into()
    }
}

impl From<Metric> for EventArray {
    fn from(metric: Metric) -> Self {
        Event::from(metric).into()
    }
}

impl From<TraceEvent> for EventArray {
    fn from(trace: TraceEvent) -> Self {
        Event::from(trace).into()
    }
}

impl From<LogBatch> for EventArray {
    fn from(batch: LogBatch) -> Self {
        Self::Logs(batch)
    }
}

impl From<Vec<LogEvent>> for EventArray {
    fn from(array: Vec<LogEvent>) -> Self {
        Self::Logs(array.into())
    }
}

impl From<MetricArray> for EventArray {
    fn from(array: MetricArray) -> Self {
        Self::Metrics(array)
    }
}

impl AddBatchNotifier for EventArray {
    fn add_batch_notifier(&mut self, batch: BatchNotifier) {
        match self {
            Self::Logs(array) => array.add_batch_notifier(batch.clone()),
            Self::Metrics(array) => array
                .iter_mut()
                .for_each(|item| item.add_finalizer(EventFinalizer::new(batch.clone()))),
            Self::Traces(array) => array
                .iter_mut()
                .for_each(|item| item.add_finalizer(EventFinalizer::new(batch.clone()))),
        }
    }
}

impl ByteSizeOf for EventArray {
    fn allocated_bytes(&self) -> usize {
        match self {
            Self::Logs(a) => a.allocated_bytes(),
            Self::Metrics(a) => a.allocated_bytes(),
            Self::Traces(a) => a.allocated_bytes(),
        }
    }
}

impl EstimatedJsonEncodedSizeOf for EventArray {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        match self {
            Self::Logs(v) => v.estimated_json_encoded_size_of(),
            Self::Traces(v) => v.estimated_json_encoded_size_of(),
            Self::Metrics(v) => v.estimated_json_encoded_size_of(),
        }
    }
}

impl EventCount for EventArray {
    fn event_count(&self) -> usize {
        match self {
            Self::Logs(a) => a.len(),
            Self::Metrics(a) => a.len(),
            Self::Traces(a) => a.len(),
        }
    }
}

impl EventContainer for EventArray {
    type IntoIter = EventArrayIntoIter;

    fn len(&self) -> usize {
        match self {
            Self::Logs(a) => a.len(),
            Self::Metrics(a) => a.len(),
            Self::Traces(a) => a.len(),
        }
    }

    fn into_events(self) -> Self::IntoIter {
        match self {
            Self::Logs(a) => EventArrayIntoIter::Logs(Vec::<LogEvent>::from(a).into_iter()),
            Self::Metrics(a) => EventArrayIntoIter::Metrics(a.into_iter()),
            Self::Traces(a) => EventArrayIntoIter::Traces(a.into_iter()),
        }
    }
}

impl EventDataEq for EventArray {
    fn event_data_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Logs(a), Self::Logs(b)) => a.event_data_eq(b),
            (Self::Metrics(a), Self::Metrics(b)) => a.event_data_eq(b),
            (Self::Traces(a), Self::Traces(b)) => a.event_data_eq(b),
            _ => false,
        }
    }
}

impl Finalizable for EventArray {
    fn take_finalizers(&mut self) -> EventFinalizers {
        match self {
            Self::Logs(a) => a.take_finalizers(),
            Self::Metrics(a) => a.iter_mut().map(Finalizable::take_finalizers).collect(),
            Self::Traces(a) => a.iter_mut().map(Finalizable::take_finalizers).collect(),
        }
    }

    fn take_finalizer_groups(&mut self) -> EventFinalizerGroups {
        match self {
            Self::Logs(a) => a.take_finalizer_groups(),
            Self::Metrics(a) => a.iter_mut().map(Finalizable::take_finalizers).collect(),
            Self::Traces(a) => a.iter_mut().map(Finalizable::take_finalizers).collect(),
        }
    }
}

impl GroupedFinalizable for EventArray {
    fn merge_finalizer_groups(&mut self, finalizers: EventFinalizerGroups) {
        fn merge_into<T: MergeFinalizable>(items: &mut [T], finalizers: EventFinalizerGroups) {
            assert_eq!(
                items.len(),
                finalizers.len(),
                "finalizer group count must match EventArray length"
            );

            for (item, finalizers) in items.iter_mut().zip(finalizers.into_groups()) {
                item.merge_finalizers(finalizers);
            }
        }

        match self {
            Self::Logs(a) => merge_into(a.as_rows_mut(), finalizers),
            Self::Metrics(a) => merge_into(a, finalizers),
            Self::Traces(a) => merge_into(a, finalizers),
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot::error::TryRecvError;
    use vector_common::finalization::{BatchStatus, EventStatus};

    use super::*;

    #[test]
    fn grouped_finalizer_round_trip_preserves_event_ownership() {
        let (first_batch, mut first_rx) = BatchNotifier::new_with_receiver();
        let (second_batch, mut second_rx) = BatchNotifier::new_with_receiver();

        let mut first = LogEvent::default();
        first.add_finalizer(EventFinalizer::new(first_batch));

        let mut second = LogEvent::default();
        second.add_finalizer(EventFinalizer::new(second_batch));

        let mut array = EventArray::Logs(vec![first, second].into());
        let finalizers = array.take_finalizer_groups();
        array.merge_finalizer_groups(finalizers);

        let mut events = array.into_events();
        let mut first = events.next().expect("first event must exist");
        let mut second = events.next().expect("second event must exist");
        assert!(events.next().is_none());

        let first_finalizers = first.take_finalizers();
        first_finalizers.update_status(EventStatus::Delivered);
        drop(first_finalizers);

        assert_eq!(first_rx.try_recv(), Ok(BatchStatus::Delivered));
        assert!(matches!(second_rx.try_recv(), Err(TryRecvError::Empty)));

        let second_finalizers = second.take_finalizers();
        second_finalizers.update_status(EventStatus::Errored);
        drop(second_finalizers);

        assert_eq!(second_rx.try_recv(), Ok(BatchStatus::Errored));
    }

    #[test]
    fn empty_event_array_grouped_round_trip() {
        let mut array = EventArray::Logs(Vec::new().into());
        let finalizers = array.take_finalizer_groups();

        assert!(finalizers.is_empty());
        array.merge_finalizer_groups(finalizers);
        assert!(array.is_empty());
    }
}

#[cfg(all(test, feature = "columnar"))]
mod columnar_tests {
    use std::sync::Arc;

    use arrow::{
        array::{Float64Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use lookup::event_path;
    use vector_common::finalization::{BatchStatus, EventStatus};

    use super::*;
    use crate::event::Value;

    fn sample_batch(prices: &[f64]) -> RecordBatch {
        let n = prices.len() as i64;
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("seq", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        let symbols = StringArray::from(vec!["AAA"; prices.len()]);
        let seqs = Int64Array::from((0..n).collect::<Vec<_>>());
        let prices = Float64Array::from(prices.to_vec());
        RecordBatch::try_new(
            schema,
            vec![Arc::new(symbols), Arc::new(seqs), Arc::new(prices)],
        )
        .unwrap()
    }

    #[test]
    fn columns_round_trip_data_and_finalizer() {
        let (notifier, mut rx) = BatchNotifier::new_with_receiver();
        let mut meta = EventMetadata::default();
        meta.add_finalizer(EventFinalizer::new(notifier));

        let batch = sample_batch(&[1.5, 2.5, 3.5]);
        let mut log_batch = LogBatch::columns(batch, BatchMetadata::Shared(meta));
        assert_eq!(LogBatch::len(&log_batch), 3);
        assert!(matches!(log_batch.repr(), LogRepr::Columns { .. }));

        // Draining finalizers yields the single shared set; acking it delivers
        // the whole batch.
        let finalizers = log_batch.take_finalizers();
        finalizers.update_status(EventStatus::Delivered);
        drop(finalizers);
        assert_eq!(rx.try_recv(), Ok(BatchStatus::Delivered));

        // Materialization reproduces the rows.
        let rows = Vec::<LogEvent>::from(log_batch);
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0].get(event_path!("symbol")),
            Some(&Value::from("AAA"))
        );
        assert_eq!(rows[1].get(event_path!("seq")), Some(&Value::from(1_i64)));
        assert_eq!(rows[2].get(event_path!("price")), Some(&Value::from(3.5)));
    }

    #[test]
    fn merge_two_columns_stays_columnar() {
        let m = BatchMetadata::Shared(EventMetadata::default());
        let mut a = LogBatch::columns(sample_batch(&[1.0, 2.0]), m.clone());
        let b = LogBatch::columns(sample_batch(&[3.0]), m);
        a.merge(b);
        assert_eq!(LogBatch::len(&a), 3);
        assert!(matches!(a.repr(), LogRepr::Columns { .. }));
    }

    #[test]
    fn clone_columnar_event_array_stays_columnar_and_log_typed() {
        let events = EventArray::Logs(LogBatch::columns(
            sample_batch(&[1.0, 2.0]),
            BatchMetadata::Shared(EventMetadata::default()),
        ));
        // Type routing sees a plain log batch (this is what `filter_events_type` matches on).
        assert!(matches!(events, EventArray::Logs(_)));

        // Fanout/buffer rely on `Clone` being a cheap Arc-bump that does not materialize.
        let cloned = events.clone();
        match &cloned {
            EventArray::Logs(batch) => assert!(matches!(batch.repr(), LogRepr::Columns { .. })),
            other => panic!("expected columnar logs, got {other:?}"),
        }
        assert_eq!(cloned, events);
    }

    #[test]
    fn stamp_columnar_metadata_stays_columnar() {
        let mut batch = LogBatch::columns(
            sample_batch(&[1.0, 2.0, 3.0]),
            BatchMetadata::Shared(EventMetadata::default()),
        );
        let mut calls = 0;
        batch.for_each_metadata_mut(|_meta| calls += 1);
        // A shared batch is stamped once for the whole batch, not per row...
        assert_eq!(calls, 1);
        // ...and stamping must not force materialization to rows.
        assert!(matches!(batch.repr(), LogRepr::Columns { .. }));
    }

    #[test]
    fn merge_columns_and_rows_materializes() {
        let mut a = LogBatch::columns(
            sample_batch(&[1.0]),
            BatchMetadata::Shared(EventMetadata::default()),
        );
        let b = LogBatch::from_rows(vec![LogEvent::default()]);
        a.merge(b);
        assert_eq!(LogBatch::len(&a), 2);
        assert!(matches!(a.repr(), LogRepr::Rows(_)));
    }
}

#[cfg(test)]
impl Arbitrary for EventArray {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = u8::arbitrary(g) as usize;
        let choice: u8 = u8::arbitrary(g);
        // Quickcheck can't derive Arbitrary for enums, see
        // https://github.com/BurntSushi/quickcheck/issues/98
        if choice.is_multiple_of(2) {
            let mut logs = Vec::new();
            for _ in 0..len {
                logs.push(LogEvent::arbitrary(g));
            }
            EventArray::Logs(logs.into())
        } else {
            let mut metrics = Vec::new();
            for _ in 0..len {
                metrics.push(Metric::arbitrary(g));
            }
            EventArray::Metrics(metrics)
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            EventArray::Logs(logs) => Box::new(
                Vec::<LogEvent>::from(logs.clone())
                    .shrink()
                    .map(|v| EventArray::Logs(v.into())),
            ),
            EventArray::Metrics(metrics) => Box::new(metrics.shrink().map(EventArray::Metrics)),
            EventArray::Traces(traces) => Box::new(traces.shrink().map(EventArray::Traces)),
        }
    }
}

/// The iterator type for `EventArray::iter_events`.
#[derive(Debug)]
pub enum EventArrayIter<'a> {
    /// An iterator over type `LogEvent`.
    Logs(slice::Iter<'a, LogEvent>),
    /// An iterator over type `Metric`.
    Metrics(slice::Iter<'a, Metric>),
    /// An iterator over type `Trace`.
    Traces(slice::Iter<'a, TraceEvent>),
}

impl<'a> Iterator for EventArrayIter<'a> {
    type Item = EventRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Logs(i) => i.next().map(EventRef::from),
            Self::Metrics(i) => i.next().map(EventRef::from),
            Self::Traces(i) => i.next().map(EventRef::from),
        }
    }
}

/// The iterator type for `EventArray::iter_events_mut`.
#[derive(Debug)]
pub enum EventArrayIterMut<'a> {
    /// An iterator over type `LogEvent`.
    Logs(slice::IterMut<'a, LogEvent>),
    /// An iterator over type `Metric`.
    Metrics(slice::IterMut<'a, Metric>),
    /// An iterator over type `Trace`.
    Traces(slice::IterMut<'a, TraceEvent>),
}

impl<'a> Iterator for EventArrayIterMut<'a> {
    type Item = EventMutRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Logs(i) => i.next().map(EventMutRef::from),
            Self::Metrics(i) => i.next().map(EventMutRef::from),
            Self::Traces(i) => i.next().map(EventMutRef::from),
        }
    }
}

/// The iterator type for `EventArray::into_events`.
#[derive(Debug)]
pub enum EventArrayIntoIter {
    /// An iterator over type `LogEvent`.
    Logs(vec::IntoIter<LogEvent>),
    /// An iterator over type `Metric`.
    Metrics(vec::IntoIter<Metric>),
    /// An iterator over type `TraceEvent`.
    Traces(vec::IntoIter<TraceEvent>),
}

impl Iterator for EventArrayIntoIter {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Logs(i) => i.next().map(Into::into),
            Self::Metrics(i) => i.next().map(Into::into),
            Self::Traces(i) => i.next().map(Event::Trace),
        }
    }
}

struct TypedArrayIterMut<'a, T>(Option<slice::IterMut<'a, T>>);

impl<'a, T> Iterator for TypedArrayIterMut<'a, T> {
    type Item = &'a mut T;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.as_mut().and_then(Iterator::next)
    }
}

/// Intermediate buffer for conversion of a sequence of individual
/// `Event`s into a sequence of `EventArray`s by coalescing contiguous
/// events of the same type into one array. This is used by
/// `events_into_array`.
#[derive(Debug, Default)]
pub struct EventArrayBuffer {
    buffer: Option<EventArray>,
    max_size: usize,
}

impl EventArrayBuffer {
    fn new(max_size: Option<usize>) -> Self {
        let max_size = max_size.unwrap_or(usize::MAX);
        let buffer = None;
        Self { buffer, max_size }
    }

    #[must_use]
    fn push(&mut self, event: Event) -> Option<EventArray> {
        match (event, &mut self.buffer) {
            (Event::Log(event), Some(EventArray::Logs(array))) if array.len() < self.max_size => {
                array.as_rows_mut().push(event);
                None
            }
            (Event::Metric(event), Some(EventArray::Metrics(array)))
                if array.len() < self.max_size =>
            {
                array.push(event);
                None
            }
            (Event::Trace(event), Some(EventArray::Traces(array)))
                if array.len() < self.max_size =>
            {
                array.push(event);
                None
            }
            (event, current) => current.replace(EventArray::from(event)),
        }
    }

    fn take(&mut self) -> Option<EventArray> {
        self.buffer.take()
    }
}

/// Convert the iterator over individual `Event`s into an iterator
/// over coalesced `EventArray`s.
pub fn events_into_arrays(
    events: impl IntoIterator<Item = Event>,
    max_size: Option<usize>,
) -> impl Iterator<Item = EventArray> {
    IntoEventArraysIter {
        inner: events.into_iter().fuse(),
        current: EventArrayBuffer::new(max_size),
    }
}

/// Iterator type implementing `into_arrays`
pub struct IntoEventArraysIter<I> {
    inner: iter::Fuse<I>,
    current: EventArrayBuffer,
}

impl<I: Iterator<Item = Event>> Iterator for IntoEventArraysIter<I> {
    type Item = EventArray;
    fn next(&mut self) -> Option<Self::Item> {
        for event in self.inner.by_ref() {
            if let Some(array) = self.current.push(event) {
                return Some(array);
            }
        }
        self.current.take()
    }
}
