//! Arrow IPC codec for vertex/edge label groups.
//!
//! Arrow columnar layout enables:
//! - Zero-copy property scan
//! - SIMD-vectorizable aggregation
//! - Vineyard/GraphScope-compatible format
//! - Lazy column access (skip unneeded properties)

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_array::{
    ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    builder::{Float64Builder, Int32Builder, Int64Builder, StringBuilder},
};
use arrow_schema::{DataType, Field, Schema};
use yata_grin::PropValue;

use crate::blocks::{EdgeBlock, LabelEdgeGroup, LabelVertexGroup, VertexBlock};

/// Arrow codec error.
#[derive(Debug, thiserror::Error)]
pub enum ArrowCodecError {
    #[error("encode error: {0}")]
    Encode(String),
    #[error("decode error: {0}")]
    Decode(String),
}

pub type Result<T> = std::result::Result<T, ArrowCodecError>;

/// Magic bytes to distinguish Arrow IPC blocks from other formats.
pub const ARROW_MAGIC: &[u8; 4] = b"ARW\x01";

// ── Vertex Group ──────────────────────────────────────────────────────

/// Encode a LabelVertexGroup to Arrow IPC bytes.
pub fn encode_vertex_group(group: &LabelVertexGroup) -> Result<Vec<u8>> {
    if group.vertices.is_empty() {
        return encode_empty_vertex_group(&group.label);
    }

    let mut all_keys: BTreeMap<String, PropType> = BTreeMap::new();
    for v in &group.vertices {
        for (k, val) in &v.props {
            all_keys.entry(k.clone()).or_insert(prop_type(val));
        }
    }

    let mut fields = vec![
        Field::new("_vid", DataType::Int32, false),
        Field::new("_labels", DataType::Utf8, false),
        Field::new("_label", DataType::Utf8, false),
    ];
    for (key, pt) in &all_keys {
        fields.push(Field::new(key, pt.to_arrow_type(), true));
    }
    let schema = Arc::new(Schema::new(fields));

    let n = group.vertices.len();
    let mut vid_col = Int32Builder::with_capacity(n);
    let mut labels_col = StringBuilder::new();
    let mut label_col = StringBuilder::new();

    let mut prop_builders: Vec<PropColumnBuilder> = all_keys
        .values()
        .map(|pt| PropColumnBuilder::new(*pt, n))
        .collect();
    let key_order: Vec<&String> = all_keys.keys().collect();

    for v in &group.vertices {
        vid_col.append_value(v.vid as i32);
        let labels_json = serde_json::to_string(&v.labels).unwrap_or_default();
        labels_col.append_value(&labels_json);
        label_col.append_value(&group.label);

        for (i, key) in key_order.iter().enumerate() {
            match v.props.iter().find(|(k, _)| k == *key) {
                Some((_, val)) => prop_builders[i].append(val),
                None => prop_builders[i].append_null(),
            }
        }
    }

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(vid_col.finish()),
        Arc::new(labels_col.finish()),
        Arc::new(label_col.finish()),
    ];
    for builder in &mut prop_builders {
        columns.push(builder.finish());
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| ArrowCodecError::Encode(format!("arrow batch: {e}")))?;

    let mut buf = Vec::with_capacity(n * 128);
    buf.extend_from_slice(ARROW_MAGIC);
    let mut writer = StreamWriter::try_new(&mut buf, &schema)
        .map_err(|e| ArrowCodecError::Encode(format!("arrow writer: {e}")))?;
    writer
        .write(&batch)
        .map_err(|e| ArrowCodecError::Encode(format!("arrow write: {e}")))?;
    writer
        .finish()
        .map_err(|e| ArrowCodecError::Encode(format!("arrow finish: {e}")))?;

    Ok(buf)
}

/// Decode a LabelVertexGroup from Arrow IPC bytes.
pub fn decode_vertex_group(data: &[u8]) -> Result<LabelVertexGroup> {
    if data.len() < 4 || &data[..4] != ARROW_MAGIC {
        return Err(ArrowCodecError::Decode("not an Arrow IPC block".into()));
    }
    let ipc_data = &data[4..];

    let reader = StreamReader::try_new(std::io::Cursor::new(ipc_data), None)
        .map_err(|e| ArrowCodecError::Decode(format!("arrow reader: {e}")))?;

    let schema = reader.schema();
    let mut vertices = Vec::new();
    let mut label = String::new();

    for batch_result in reader {
        let batch =
            batch_result.map_err(|e| ArrowCodecError::Decode(format!("arrow batch: {e}")))?;
        let n = batch.num_rows();

        let vid_col = batch
            .column_by_name("_vid")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .ok_or_else(|| ArrowCodecError::Decode("missing _vid column".into()))?;
        let labels_col = batch
            .column_by_name("_labels")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| ArrowCodecError::Decode("missing _labels column".into()))?;

        if label.is_empty() {
            if let Some(lc) = batch
                .column_by_name("_label")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            {
                if n > 0 {
                    label = lc.value(0).to_string();
                }
            }
        }

        const INTERNAL_COLS: &[&str] = &["_vid", "_labels", "_label"];
        let prop_fields: Vec<&Arc<Field>> = schema
            .fields()
            .iter()
            .filter(|f| !INTERNAL_COLS.contains(&f.name().as_str()))
            .collect();

        for row in 0..n {
            let vid = vid_col.value(row) as u32;
            let labels: Vec<String> =
                serde_json::from_str(labels_col.value(row)).unwrap_or_default();
            let mut props = Vec::new();

            for field in &prop_fields {
                let col = batch.column_by_name(field.name()).unwrap();
                if col.is_null(row) {
                    continue;
                }
                if let Some(val) = read_prop_value(col, row, field.data_type()) {
                    props.push((field.name().clone(), val));
                }
            }
            props.sort_by(|a, b| a.0.cmp(&b.0));
            vertices.push(VertexBlock { vid, labels, props });
        }
    }

    Ok(LabelVertexGroup {
        count: vertices.len() as u32,
        label,
        vertices,
    })
}

// ── Edge Group ────────────────────────────────────────────────────────

/// Encode a LabelEdgeGroup to Arrow IPC bytes.
pub fn encode_edge_group(group: &LabelEdgeGroup) -> Result<Vec<u8>> {
    let n = group.edges.len();

    let mut all_keys: BTreeMap<String, PropType> = BTreeMap::new();
    for e in &group.edges {
        for (k, val) in &e.props {
            all_keys.entry(k.clone()).or_insert(prop_type(val));
        }
    }

    let mut fields = vec![
        Field::new("_edge_id", DataType::Int32, false),
        Field::new("_src", DataType::Int32, false),
        Field::new("_dst", DataType::Int32, false),
        Field::new("_label", DataType::Utf8, false),
    ];
    for (key, pt) in &all_keys {
        fields.push(Field::new(key, pt.to_arrow_type(), true));
    }
    let schema = Arc::new(Schema::new(fields));

    let mut eid_col = Int32Builder::with_capacity(n);
    let mut src_col = Int32Builder::with_capacity(n);
    let mut dst_col = Int32Builder::with_capacity(n);
    let mut label_col = StringBuilder::new();

    let mut prop_builders: Vec<PropColumnBuilder> = all_keys
        .values()
        .map(|pt| PropColumnBuilder::new(*pt, n))
        .collect();
    let key_order: Vec<&String> = all_keys.keys().collect();

    for e in &group.edges {
        eid_col.append_value(e.edge_id as i32);
        src_col.append_value(e.src as i32);
        dst_col.append_value(e.dst as i32);
        label_col.append_value(&e.label);

        for (i, key) in key_order.iter().enumerate() {
            match e.props.iter().find(|(k, _)| k == *key) {
                Some((_, val)) => prop_builders[i].append(val),
                None => prop_builders[i].append_null(),
            }
        }
    }

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(eid_col.finish()),
        Arc::new(src_col.finish()),
        Arc::new(dst_col.finish()),
        Arc::new(label_col.finish()),
    ];
    for builder in &mut prop_builders {
        columns.push(builder.finish());
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| ArrowCodecError::Encode(format!("arrow edge batch: {e}")))?;

    let mut buf = Vec::with_capacity(n * 64);
    buf.extend_from_slice(ARROW_MAGIC);
    let mut writer = StreamWriter::try_new(&mut buf, &schema)
        .map_err(|e| ArrowCodecError::Encode(format!("arrow writer: {e}")))?;
    writer
        .write(&batch)
        .map_err(|e| ArrowCodecError::Encode(format!("arrow write: {e}")))?;
    writer
        .finish()
        .map_err(|e| ArrowCodecError::Encode(format!("arrow finish: {e}")))?;
    Ok(buf)
}

/// Decode a LabelEdgeGroup from Arrow IPC bytes.
pub fn decode_edge_group(data: &[u8]) -> Result<LabelEdgeGroup> {
    if data.len() < 4 || &data[..4] != ARROW_MAGIC {
        return Err(ArrowCodecError::Decode(
            "not an Arrow IPC edge block".into(),
        ));
    }
    let reader = StreamReader::try_new(std::io::Cursor::new(&data[4..]), None)
        .map_err(|e| ArrowCodecError::Decode(format!("arrow reader: {e}")))?;

    let schema = reader.schema();
    let mut edges = Vec::new();
    let mut label = String::new();

    for batch_result in reader {
        let batch =
            batch_result.map_err(|e| ArrowCodecError::Decode(format!("arrow batch: {e}")))?;
        let n = batch.num_rows();

        let eid_col = batch
            .column_by_name("_edge_id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let src_col = batch
            .column_by_name("_src")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let dst_col = batch
            .column_by_name("_dst")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();

        if label.is_empty() {
            if let Some(lc) = batch
                .column_by_name("_label")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            {
                if n > 0 {
                    label = lc.value(0).to_string();
                }
            }
        }

        const INTERNAL_COLS: &[&str] = &["_edge_id", "_src", "_dst", "_label"];
        let prop_fields: Vec<&Arc<Field>> = schema
            .fields()
            .iter()
            .filter(|f| !INTERNAL_COLS.contains(&f.name().as_str()))
            .collect();

        for row in 0..n {
            let mut props = Vec::new();
            for field in &prop_fields {
                let col = batch.column_by_name(field.name()).unwrap();
                if col.is_null(row) {
                    continue;
                }
                if let Some(val) = read_prop_value(col, row, field.data_type()) {
                    props.push((field.name().clone(), val));
                }
            }
            props.sort_by(|a, b| a.0.cmp(&b.0));
            edges.push(EdgeBlock {
                edge_id: eid_col.value(row) as u32,
                src: src_col.value(row) as u32,
                dst: dst_col.value(row) as u32,
                label: label.clone(),
                props,
            });
        }
    }

    Ok(LabelEdgeGroup {
        count: edges.len() as u32,
        label,
        edges,
    })
}

/// Check if data is Arrow IPC format.
pub fn is_arrow_block(data: &[u8]) -> bool {
    data.len() >= 4 && &data[..4] == ARROW_MAGIC
}

// ── Internal helpers ──────────────────────────────────────────────────

fn encode_empty_vertex_group(label: &str) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("_vid", DataType::Int32, false),
        Field::new("_labels", DataType::Utf8, false),
        Field::new("_label", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(vec![label; 0])),
        ],
    )
    .map_err(|e| ArrowCodecError::Encode(e.to_string()))?;

    let mut buf = Vec::new();
    buf.extend_from_slice(ARROW_MAGIC);
    let mut writer = StreamWriter::try_new(&mut buf, &schema)
        .map_err(|e| ArrowCodecError::Encode(e.to_string()))?;
    writer
        .write(&batch)
        .map_err(|e| ArrowCodecError::Encode(e.to_string()))?;
    writer
        .finish()
        .map_err(|e| ArrowCodecError::Encode(e.to_string()))?;
    Ok(buf)
}

#[derive(Debug, Clone, Copy)]
enum PropType {
    Int,
    Float,
    Str,
}

impl PropType {
    fn to_arrow_type(self) -> DataType {
        match self {
            PropType::Int => DataType::Int64,
            PropType::Float => DataType::Float64,
            PropType::Str => DataType::Utf8,
        }
    }
}

fn prop_type(val: &PropValue) -> PropType {
    match val {
        PropValue::Int(_) => PropType::Int,
        PropValue::Float(_) => PropType::Float,
        _ => PropType::Str,
    }
}

enum PropColumnBuilder {
    Int(Int64Builder),
    Float(Float64Builder),
    Str(StringBuilder),
}

impl PropColumnBuilder {
    fn new(pt: PropType, cap: usize) -> Self {
        match pt {
            PropType::Int => Self::Int(Int64Builder::with_capacity(cap)),
            PropType::Float => Self::Float(Float64Builder::with_capacity(cap)),
            PropType::Str => Self::Str(StringBuilder::new()),
        }
    }

    fn append(&mut self, val: &PropValue) {
        match (self, val) {
            (Self::Int(b), PropValue::Int(v)) => b.append_value(*v),
            (Self::Float(b), PropValue::Float(v)) => b.append_value(*v),
            (Self::Str(b), PropValue::Str(v)) => b.append_value(v),
            (Self::Str(b), PropValue::Bool(v)) => b.append_value(if *v { "true" } else { "false" }),
            (Self::Str(b), PropValue::Int(v)) => b.append_value(&v.to_string()),
            (Self::Str(b), PropValue::Float(v)) => b.append_value(&v.to_string()),
            (Self::Int(b), _) => b.append_null(),
            (Self::Float(b), _) => b.append_null(),
            (Self::Str(b), _) => b.append_null(),
        }
    }

    fn append_null(&mut self) {
        match self {
            Self::Int(b) => b.append_null(),
            Self::Float(b) => b.append_null(),
            Self::Str(b) => b.append_null(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Int(b) => Arc::new(b.finish()),
            Self::Float(b) => Arc::new(b.finish()),
            Self::Str(b) => Arc::new(b.finish()),
        }
    }
}

fn read_prop_value(col: &ArrayRef, row: usize, dt: &DataType) -> Option<PropValue> {
    match dt {
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| PropValue::Int(a.value(row))),
        DataType::Int32 => col
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| PropValue::Int(a.value(row) as i64)),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| PropValue::Float(a.value(row))),
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| PropValue::Str(a.value(row).to_string())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vertex_group_arrow_roundtrip() {
        let group = LabelVertexGroup {
            label: "Vehicle".into(),
            count: 2,
            vertices: vec![
                VertexBlock {
                    vid: 0,
                    labels: vec!["Vehicle".into()],
                    props: vec![
                        ("horsepower".into(), PropValue::Int(382)),
                        ("make".into(), PropValue::Str("Toyota".into())),
                        ("price".into(), PropValue::Float(7313000.0)),
                    ],
                },
                VertexBlock {
                    vid: 1,
                    labels: vec!["Vehicle".into()],
                    props: vec![
                        ("horsepower".into(), PropValue::Int(570)),
                        ("make".into(), PropValue::Str("Nissan".into())),
                        ("price".into(), PropValue::Float(13156000.0)),
                    ],
                },
            ],
        };

        let bytes = encode_vertex_group(&group).unwrap();
        assert!(is_arrow_block(&bytes));

        let decoded = decode_vertex_group(&bytes).unwrap();
        assert_eq!(decoded.label, "Vehicle");
        assert_eq!(decoded.count, 2);
        assert_eq!(decoded.vertices.len(), 2);
        assert_eq!(decoded.vertices[0].vid, 0);
        assert_eq!(decoded.vertices[1].vid, 1);
    }

    #[test]
    fn test_edge_group_arrow_roundtrip() {
        let group = LabelEdgeGroup {
            label: "REVIEWS".into(),
            count: 1,
            edges: vec![EdgeBlock {
                edge_id: 0,
                src: 5,
                dst: 0,
                label: "REVIEWS".into(),
                props: vec![("rating".into(), PropValue::Float(4.5))],
            }],
        };

        let bytes = encode_edge_group(&group).unwrap();
        assert!(is_arrow_block(&bytes));

        let decoded = decode_edge_group(&bytes).unwrap();
        assert_eq!(decoded.label, "REVIEWS");
        assert_eq!(decoded.edges.len(), 1);
        assert_eq!(decoded.edges[0].src, 5);
    }

    #[test]
    fn test_empty_vertex_group() {
        let group = LabelVertexGroup {
            label: "Empty".into(),
            count: 0,
            vertices: vec![],
        };
        let bytes = encode_vertex_group(&group).unwrap();
        let decoded = decode_vertex_group(&bytes).unwrap();
        assert_eq!(decoded.vertices.len(), 0);
    }
}
