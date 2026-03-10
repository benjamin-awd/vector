use criterion::criterion_main;

mod arrow_deserializer;
mod character_delimited_bytes;
mod encoder;
mod newline_bytes;

criterion_main!(
    arrow_deserializer::benches,
    character_delimited_bytes::benches,
    newline_bytes::benches,
    encoder::benches,
);
