//! PropValue ↔ DurablePropValue conversions.
//!
//! Free functions (not From impls) due to orphan rule:
//! both PropValue (yata-grin) and DurablePropValue (yata-core) are external.

use yata_core::DurablePropValue;
use yata_grin::PropValue;

pub fn prop_to_durable(v: &PropValue) -> DurablePropValue {
    match v {
        PropValue::Null => DurablePropValue::Null,
        PropValue::Bool(b) => DurablePropValue::Bool(*b),
        PropValue::Int(i) => DurablePropValue::Int(*i),
        PropValue::Float(f) => DurablePropValue::Float(*f),
        PropValue::Str(s) => DurablePropValue::Str(s.clone()),
    }
}

pub fn durable_to_prop(v: &DurablePropValue) -> PropValue {
    match v {
        DurablePropValue::Null => PropValue::Null,
        DurablePropValue::Bool(b) => PropValue::Bool(*b),
        DurablePropValue::Int(i) => PropValue::Int(*i),
        DurablePropValue::Float(f) => PropValue::Float(*f),
        DurablePropValue::Str(s) => PropValue::Str(s.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_all_types() {
        let cases = vec![
            PropValue::Null,
            PropValue::Bool(true),
            PropValue::Int(42),
            PropValue::Float(3.14),
            PropValue::Str("hello".into()),
        ];
        for orig in cases {
            let durable = prop_to_durable(&orig);
            let back = durable_to_prop(&durable);
            assert_eq!(orig, back);
        }
    }
}
