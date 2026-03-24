//! Integration tests for yata-vineyard ArrowFragment.

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use std::sync::Arc;

use crate::blob::{BlobStore, MemoryBlobStore};
use crate::fragment::{ArrowFragment, split_record_batch};
use crate::nbr::{self, NbrUnit64};
use crate::schema::PropertyGraphSchema;

/// Build a test graph:
/// - 3 Person vertices (id=0,1,2) with name + age props
/// - 2 KNOWS edges (0→1, 1→2) with weight prop
/// - Directed, single fragment
fn build_test_fragment() -> ArrowFragment {
    let mut schema = PropertyGraphSchema::new();
    let vlabel = schema.add_vertex_label("Person");
    schema
        .vertex_entry_mut(vlabel)
        .unwrap()
        .add_prop("name", &DataType::Utf8);
    schema
        .vertex_entry_mut(vlabel)
        .unwrap()
        .add_prop("age", &DataType::Int64);

    let elabel = schema.add_edge_label("KNOWS");
    schema
        .edge_entry_mut(elabel)
        .unwrap()
        .add_prop("weight", &DataType::Float64);

    let mut frag = ArrowFragment::new(0, 1, true, schema);

    // Vertex table: Person
    let vertex_schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
    ]));
    let names = StringArray::from(vec!["Alice", "Bob", "Carol"]);
    let ages = Int64Array::from(vec![30, 25, 35]);
    let vertex_batch =
        RecordBatch::try_new(vertex_schema, vec![Arc::new(names), Arc::new(ages)]).unwrap();
    frag.vertex_tables.push(vertex_batch);
    frag.ivnums[0] = 3;

    // Edge table: KNOWS
    let edge_schema = Arc::new(Schema::new(vec![Field::new(
        "weight",
        DataType::Float64,
        false,
    )]));
    let weights = Float64Array::from(vec![0.8, 0.9]);
    let edge_batch = RecordBatch::try_new(edge_schema, vec![Arc::new(weights)]).unwrap();
    frag.edge_tables.push(edge_batch);

    // CSR: OE offsets for Person×KNOWS
    // Vertex 0 → 1 edge (eid=0, dst=1)
    // Vertex 1 → 1 edge (eid=1, dst=2)
    // Vertex 2 → 0 edges
    // offsets = [0, 1, 2, 2]
    let oe_offsets = Int64Array::from(vec![0i64, 1, 2, 2]);
    let oe_units = vec![NbrUnit64::new(1, 0), NbrUnit64::new(2, 1)];
    let oe_bytes = Bytes::copy_from_slice(nbr::nbr_units_to_bytes(&oe_units));

    frag.oe_offsets[0][0] = Some(oe_offsets);
    frag.oe_nbrs[0][0] = Some(oe_bytes);

    // CSR: IE offsets for Person×KNOWS
    // Vertex 0 → 0 incoming
    // Vertex 1 → 1 incoming (from 0, eid=0)
    // Vertex 2 → 1 incoming (from 1, eid=1)
    // offsets = [0, 0, 1, 2]
    let ie_offsets = Int64Array::from(vec![0i64, 0, 1, 2]);
    let ie_units = vec![NbrUnit64::new(0, 0), NbrUnit64::new(1, 1)];
    let ie_bytes = Bytes::copy_from_slice(nbr::nbr_units_to_bytes(&ie_units));

    frag.ie_offsets[0][0] = Some(ie_offsets);
    frag.ie_nbrs[0][0] = Some(ie_bytes);

    frag
}

#[test]
fn fragment_basic_properties() {
    let frag = build_test_fragment();
    assert_eq!(frag.fid, 0);
    assert_eq!(frag.fnum, 1);
    assert!(frag.directed);
    assert_eq!(frag.vertex_label_num(), 1);
    assert_eq!(frag.edge_label_num(), 1);
    assert_eq!(frag.inner_vertex_num(0), 3);
    assert_eq!(frag.edge_num(), 2);
}

#[test]
fn fragment_vertex_table_access() {
    let frag = build_test_fragment();
    let batch = frag.vertex_table(0).unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);

    let names = batch
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Alice");
    assert_eq!(names.value(1), "Bob");
    assert_eq!(names.value(2), "Carol");

    let ages = batch
        .column_by_name("age")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ages.value(0), 30);
    assert_eq!(ages.value(1), 25);
}

#[test]
fn fragment_csr_traversal() {
    let frag = build_test_fragment();

    // Out-neighbors of vertex 0 via KNOWS
    let nbrs = frag.out_neighbors(0, 0, 0).unwrap();
    assert_eq!(nbrs.len(), 1);
    assert_eq!(nbrs[0].vid, 1);
    assert_eq!(nbrs[0].eid, 0);

    // Out-neighbors of vertex 1 via KNOWS
    let nbrs = frag.out_neighbors(0, 0, 1).unwrap();
    assert_eq!(nbrs.len(), 1);
    assert_eq!(nbrs[0].vid, 2);
    assert_eq!(nbrs[0].eid, 1);

    // Out-neighbors of vertex 2 via KNOWS (none)
    let nbrs = frag.out_neighbors(0, 0, 2).unwrap();
    assert_eq!(nbrs.len(), 0);

    // In-neighbors of vertex 1
    let nbrs = frag.in_neighbors(0, 0, 1).unwrap();
    assert_eq!(nbrs.len(), 1);
    assert_eq!(nbrs[0].vid, 0);

    // In-neighbors of vertex 2
    let nbrs = frag.in_neighbors(0, 0, 2).unwrap();
    assert_eq!(nbrs.len(), 1);
    assert_eq!(nbrs[0].vid, 1);
}

#[test]
fn fragment_serialize_deserialize_roundtrip() {
    let frag = build_test_fragment();
    let store = MemoryBlobStore::new();

    // Serialize
    let meta = frag.serialize(&store);
    assert_eq!(meta.typename, "vineyard::ArrowFragment<int64,uint64>");

    // Deserialize
    let restored = ArrowFragment::deserialize(&meta, &store).unwrap();

    // Verify structure
    assert_eq!(restored.fid, frag.fid);
    assert_eq!(restored.fnum, frag.fnum);
    assert_eq!(restored.directed, frag.directed);
    assert_eq!(restored.vertex_label_num(), frag.vertex_label_num());
    assert_eq!(restored.edge_label_num(), frag.edge_label_num());
    assert_eq!(restored.inner_vertex_num(0), 3);
    assert_eq!(restored.edge_num(), 2);

    // Verify vertex data
    let batch = restored.vertex_table(0).unwrap();
    let names = batch
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "Alice");
    assert_eq!(names.value(2), "Carol");

    // Verify CSR topology
    let nbrs = restored.out_neighbors(0, 0, 0).unwrap();
    assert_eq!(nbrs.len(), 1);
    assert_eq!(nbrs[0].vid, 1);
    assert_eq!(nbrs[0].eid, 0);

    let nbrs = restored.in_neighbors(0, 0, 2).unwrap();
    assert_eq!(nbrs.len(), 1);
    assert_eq!(nbrs[0].vid, 1);
}

#[test]
fn fragment_edge_table_access() {
    let frag = build_test_fragment();
    let batch = frag.edge_table(0).unwrap();
    assert_eq!(batch.num_rows(), 2);

    let weights = batch
        .column_by_name("weight")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((weights.value(0) - 0.8).abs() < f64::EPSILON);
    assert!((weights.value(1) - 0.9).abs() < f64::EPSILON);
}

#[test]
fn schema_from_fragment() {
    let frag = build_test_fragment();
    assert_eq!(frag.schema.vertex_labels(), vec!["Person"]);
    assert_eq!(frag.schema.edge_labels(), vec!["KNOWS"]);

    let person = frag.schema.vertex_entry(0).unwrap();
    assert_eq!(person.props.len(), 2);
    assert_eq!(person.props[0].name, "name");
    assert_eq!(person.props[0].arrow_type(), DataType::Utf8);
    assert_eq!(person.props[1].name, "age");
    assert_eq!(person.props[1].arrow_type(), DataType::Int64);

    let knows = frag.schema.edge_entry(0).unwrap();
    assert_eq!(knows.props.len(), 1);
    assert_eq!(knows.props[0].name, "weight");
    assert_eq!(knows.props[0].arrow_type(), DataType::Float64);
}

#[test]
fn blob_store_name_based() {
    let store = MemoryBlobStore::new();
    store.put("a", Bytes::from_static(b"hello"));
    store.put("b", Bytes::from_static(b"world"));

    assert_eq!(store.get("a").unwrap(), &b"hello"[..]);
    assert_eq!(store.get("b").unwrap(), &b"world"[..]);
    assert!(store.get("c").is_none());

    // Overwrite
    store.put("a", Bytes::from_static(b"updated"));
    assert_eq!(store.get("a").unwrap(), &b"updated"[..]);
}

#[test]
fn empty_fragment_roundtrip() {
    let schema = PropertyGraphSchema::new();
    let frag = ArrowFragment::new(0, 1, true, schema);
    let store = MemoryBlobStore::new();

    let meta = frag.serialize(&store);
    let restored = ArrowFragment::deserialize(&meta, &store).unwrap();

    assert_eq!(restored.vertex_label_num(), 0);
    assert_eq!(restored.edge_label_num(), 0);
    assert_eq!(restored.edge_num(), 0);
}

#[test]
fn multi_label_fragment() {
    let mut schema = PropertyGraphSchema::new();
    let person_id = schema.add_vertex_label("Person");
    schema
        .vertex_entry_mut(person_id)
        .unwrap()
        .add_prop("name", &DataType::Utf8);
    let company_id = schema.add_vertex_label("Company");
    schema
        .vertex_entry_mut(company_id)
        .unwrap()
        .add_prop("industry", &DataType::Utf8);

    let _knows_id = schema.add_edge_label("KNOWS");
    let _works_id = schema.add_edge_label("WORKS_AT");

    let frag = ArrowFragment::new(0, 1, true, schema);
    assert_eq!(frag.vertex_label_num(), 2);
    assert_eq!(frag.edge_label_num(), 2);

    let store = MemoryBlobStore::new();
    let meta = frag.serialize(&store);
    let restored = ArrowFragment::deserialize(&meta, &store).unwrap();
    assert_eq!(restored.vertex_label_num(), 2);
    assert_eq!(restored.edge_label_num(), 2);
    assert_eq!(
        restored.schema.vertex_label_name(0),
        Some("Person")
    );
    assert_eq!(
        restored.schema.vertex_label_name(1),
        Some("Company")
    );
}
