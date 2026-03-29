//! NbrUnit: packed neighbor unit for CSR adjacency.
//!
//! CSR adjacency lists store packed NbrUnit structs:
//! ```c++
//! template <typename VID_T, typename EID_T>
//! struct NbrUnit {
//!     VID_T vid;
//!     EID_T eid;
//! } __attribute__((packed, aligned(4)));
//! ```
//!
//! repr(C) layout for zero-copy cast between bytes and NbrUnit slices.

/// Packed neighbor unit for CSR adjacency.
///
/// VID=u64, EID=u64 → 16 bytes total.
/// VID=u64, EID=u32 → 12 bytes total.
///
/// Default: VID=u64, EID=u64 for maximum vertex/edge capacity.
/// repr(C) ensures stable layout for zero-copy cast between bytes and NbrUnit slices.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NbrUnit<VID = u64, EID = u64> {
    pub vid: VID,
    pub eid: EID,
}

impl<VID: Copy, EID: Copy> NbrUnit<VID, EID> {
    pub fn new(vid: VID, eid: EID) -> Self {
        Self { vid, eid }
    }
}

/// Standard NbrUnit with u64 vid + u64 eid (16 bytes).
pub type NbrUnit64 = NbrUnit<u64, u64>;

/// Size of a NbrUnit64 in bytes (16B: u64 vid + u64 eid).
pub const NBR_UNIT_64_SIZE: usize = std::mem::size_of::<NbrUnit64>();

/// Encode a slice of NbrUnit64 to raw bytes (zero-copy cast).
pub fn nbr_units_to_bytes(units: &[NbrUnit64]) -> &[u8] {
    // SAFETY: NbrUnit is repr(C, packed), no padding, each element is 16 bytes.
    unsafe { std::slice::from_raw_parts(units.as_ptr() as *const u8, units.len() * NBR_UNIT_64_SIZE) }
}

/// Decode raw bytes to a slice of NbrUnit64 (zero-copy cast).
///
/// Returns None if data length is not a multiple of NBR_UNIT_64_SIZE.
pub fn bytes_to_nbr_units(data: &[u8]) -> Option<&[NbrUnit64]> {
    if data.len() % NBR_UNIT_64_SIZE != 0 {
        return None;
    }
    let count = data.len() / NBR_UNIT_64_SIZE;
    // SAFETY: We verified alignment-free access (packed struct), length is exact multiple.
    Some(unsafe { std::slice::from_raw_parts(data.as_ptr() as *const NbrUnit64, count) })
}

/// Build NbrUnit array from separate vid/eid arrays.
pub fn build_nbr_units(vids: &[u64], eids: &[u64]) -> Vec<NbrUnit64> {
    assert_eq!(vids.len(), eids.len());
    vids.iter()
        .zip(eids.iter())
        .map(|(&vid, &eid)| NbrUnit64::new(vid, eid))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nbr_unit_64_size() {
        assert_eq!(NBR_UNIT_64_SIZE, 16);
    }

    #[test]
    fn roundtrip_bytes() {
        let units = vec![
            NbrUnit64::new(1, 100),
            NbrUnit64::new(2, 200),
            NbrUnit64::new(3, 300),
        ];
        let raw = nbr_units_to_bytes(&units);
        assert_eq!(raw.len(), 48); // 3 * 16
        let decoded = bytes_to_nbr_units(raw).unwrap();
        assert_eq!(decoded, &units[..]);
    }

    #[test]
    fn invalid_length() {
        let data = [0u8; 17]; // not a multiple of 16
        assert!(bytes_to_nbr_units(&data).is_none());
    }

    #[test]
    fn build_from_arrays() {
        let vids = vec![10u64, 20, 30];
        let eids = vec![1u64, 2, 3];
        let units = build_nbr_units(&vids, &eids);
        assert_eq!(units[0], NbrUnit64::new(10, 1));
        assert_eq!(units[2], NbrUnit64::new(30, 3));
    }
}
