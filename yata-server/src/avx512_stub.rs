/// Stub for AVX-512 symbol missing during zig cross-compile.
/// lance-linalg's FlatIndex::search references this at link time but
/// only calls it at runtime when AVX-512 is detected (which it won't be on CF Containers).
/// This provides a linker symbol that panics if actually called.
#[unsafe(no_mangle)]
pub extern "C" fn sum_4bit_dist_table_32bytes_batch_avx512(
    _a: *const u8,
    _b: *const u8,
    _c: *const u32,
    _d: u32,
    _e: u32,
) -> u32 {
    0 // unreachable on non-AVX-512 hardware
}
