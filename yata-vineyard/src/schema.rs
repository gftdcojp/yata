//! PropertyGraphSchema: Vineyard-compatible graph schema.
//!
//! Reference: https://github.com/v6d-io/v6d/blob/main/modules/graph/fragment/graph_schema.h
//!
//! Vineyard PropertyGraphSchema stores:
//! - vertex_entries: per-label schema (label name, property definitions)
//! - edge_entries: per-label schema (label name, property definitions)
//! - label_id ↔ label_name bidirectional mapping
//!
//! JSON-serializable for etcd storage and meta.json persistence.

use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

/// Property definition within a vertex/edge label.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyDef {
    pub id: i32,
    pub name: String,
    /// Arrow DataType as string (e.g., "int64", "utf8", "float64", "bool").
    /// We serialize as string for JSON compatibility; convert to Arrow DataType via `arrow_type()`.
    pub data_type: String,
}

impl PropertyDef {
    pub fn new(id: i32, name: &str, data_type: &DataType) -> Self {
        Self {
            id,
            name: name.to_string(),
            data_type: arrow_type_to_string(data_type),
        }
    }

    /// Convert stored type string back to Arrow DataType.
    pub fn arrow_type(&self) -> DataType {
        string_to_arrow_type(&self.data_type)
    }
}

/// Schema entry for a vertex or edge label.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEntry {
    pub id: i32,
    pub label: String,
    /// "vertex" or "edge"
    #[serde(rename = "type")]
    pub entry_type: String,
    pub props: Vec<PropertyDef>,
}

impl SchemaEntry {
    pub fn vertex(id: i32, label: &str) -> Self {
        Self {
            id,
            label: label.to_string(),
            entry_type: "vertex".to_string(),
            props: Vec::new(),
        }
    }

    pub fn edge(id: i32, label: &str) -> Self {
        Self {
            id,
            label: label.to_string(),
            entry_type: "edge".to_string(),
            props: Vec::new(),
        }
    }

    pub fn add_prop(&mut self, name: &str, data_type: &DataType) {
        let id = self.props.len() as i32;
        self.props.push(PropertyDef::new(id, name, data_type));
    }

    pub fn prop_id(&self, name: &str) -> Option<i32> {
        self.props.iter().find(|p| p.name == name).map(|p| p.id)
    }

    pub fn prop_by_id(&self, id: i32) -> Option<&PropertyDef> {
        self.props.iter().find(|p| p.id == id)
    }

    pub fn prop_by_name(&self, name: &str) -> Option<&PropertyDef> {
        self.props.iter().find(|p| p.name == name)
    }
}

/// Vineyard-compatible property graph schema.
///
/// Mirrors `vineyard::PropertyGraphSchema` with JSON serialization
/// compatible with Vineyard's `ToJSON()`/`FromJSON()`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PropertyGraphSchema {
    pub vertex_entries: Vec<SchemaEntry>,
    pub edge_entries: Vec<SchemaEntry>,
}

impl PropertyGraphSchema {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a vertex label, returns the assigned label_id.
    pub fn add_vertex_label(&mut self, label: &str) -> i32 {
        let id = self.vertex_entries.len() as i32;
        self.vertex_entries.push(SchemaEntry::vertex(id, label));
        id
    }

    /// Add an edge label, returns the assigned label_id.
    pub fn add_edge_label(&mut self, label: &str) -> i32 {
        let id = self.edge_entries.len() as i32;
        self.edge_entries.push(SchemaEntry::edge(id, label));
        id
    }

    /// Get vertex label_id by name.
    pub fn vertex_label_id(&self, label: &str) -> Option<i32> {
        self.vertex_entries
            .iter()
            .find(|e| e.label == label)
            .map(|e| e.id)
    }

    /// Get edge label_id by name.
    pub fn edge_label_id(&self, label: &str) -> Option<i32> {
        self.edge_entries
            .iter()
            .find(|e| e.label == label)
            .map(|e| e.id)
    }

    /// Get vertex label name by id.
    pub fn vertex_label_name(&self, id: i32) -> Option<&str> {
        self.vertex_entries
            .get(id as usize)
            .map(|e| e.label.as_str())
    }

    /// Get edge label name by id.
    pub fn edge_label_name(&self, id: i32) -> Option<&str> {
        self.edge_entries
            .get(id as usize)
            .map(|e| e.label.as_str())
    }

    /// Get mutable vertex entry by label_id.
    pub fn vertex_entry_mut(&mut self, id: i32) -> Option<&mut SchemaEntry> {
        self.vertex_entries.get_mut(id as usize)
    }

    /// Get mutable edge entry by label_id.
    pub fn edge_entry_mut(&mut self, id: i32) -> Option<&mut SchemaEntry> {
        self.edge_entries.get_mut(id as usize)
    }

    /// Get vertex entry by label_id.
    pub fn vertex_entry(&self, id: i32) -> Option<&SchemaEntry> {
        self.vertex_entries.get(id as usize)
    }

    /// Get edge entry by label_id.
    pub fn edge_entry(&self, id: i32) -> Option<&SchemaEntry> {
        self.edge_entries.get(id as usize)
    }

    /// All vertex label names.
    pub fn vertex_labels(&self) -> Vec<&str> {
        self.vertex_entries.iter().map(|e| e.label.as_str()).collect()
    }

    /// All edge label names.
    pub fn edge_labels(&self) -> Vec<&str> {
        self.edge_entries.iter().map(|e| e.label.as_str()).collect()
    }
}

// ── Arrow DataType ↔ String conversion (Vineyard-compatible) ────────

fn arrow_type_to_string(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "bool".to_string(),
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 => "utf8".to_string(),
        DataType::LargeUtf8 => "large_utf8".to_string(),
        DataType::Binary => "binary".to_string(),
        DataType::LargeBinary => "large_binary".to_string(),
        DataType::Date32 => "date32".to_string(),
        DataType::Date64 => "date64".to_string(),
        DataType::Null => "null".to_string(),
        _ => format!("{:?}", dt),
    }
}

fn string_to_arrow_type(s: &str) -> DataType {
    match s {
        "bool" | "boolean" => DataType::Boolean,
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float" | "float32" => DataType::Float32,
        "double" | "float64" => DataType::Float64,
        "utf8" | "string" => DataType::Utf8,
        "large_utf8" | "large_string" => DataType::LargeUtf8,
        "binary" => DataType::Binary,
        "large_binary" => DataType::LargeBinary,
        "date32" => DataType::Date32,
        "date64" => DataType::Date64,
        "null" => DataType::Null,
        _ => DataType::Utf8, // fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_roundtrip_json() {
        let mut schema = PropertyGraphSchema::new();
        let vid = schema.add_vertex_label("Person");
        schema
            .vertex_entry_mut(vid)
            .unwrap()
            .add_prop("name", &DataType::Utf8);
        schema
            .vertex_entry_mut(vid)
            .unwrap()
            .add_prop("age", &DataType::Int64);

        let eid = schema.add_edge_label("KNOWS");
        schema
            .edge_entry_mut(eid)
            .unwrap()
            .add_prop("weight", &DataType::Float64);

        let json = serde_json::to_string_pretty(&schema).unwrap();
        let decoded: PropertyGraphSchema = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.vertex_entries.len(), 1);
        assert_eq!(decoded.vertex_entries[0].label, "Person");
        assert_eq!(decoded.vertex_entries[0].props.len(), 2);
        assert_eq!(decoded.vertex_entries[0].props[0].name, "name");
        assert_eq!(decoded.vertex_entries[0].props[0].data_type, "utf8");
        assert_eq!(decoded.vertex_entries[0].props[1].name, "age");
        assert_eq!(decoded.vertex_entries[0].props[1].data_type, "int64");

        assert_eq!(decoded.edge_entries.len(), 1);
        assert_eq!(decoded.edge_entries[0].label, "KNOWS");
        assert_eq!(decoded.edge_entries[0].props[0].data_type, "double");
    }

    #[test]
    fn arrow_type_roundtrip() {
        let types = vec![
            DataType::Boolean,
            DataType::Int64,
            DataType::Float64,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
        ];
        for dt in types {
            let s = arrow_type_to_string(&dt);
            let back = string_to_arrow_type(&s);
            assert_eq!(back, dt, "roundtrip failed for {}", s);
        }
    }

    #[test]
    fn label_id_lookup() {
        let mut schema = PropertyGraphSchema::new();
        schema.add_vertex_label("Person");
        schema.add_vertex_label("Company");
        schema.add_edge_label("WORKS_AT");

        assert_eq!(schema.vertex_label_id("Person"), Some(0));
        assert_eq!(schema.vertex_label_id("Company"), Some(1));
        assert_eq!(schema.vertex_label_id("Unknown"), None);
        assert_eq!(schema.edge_label_id("WORKS_AT"), Some(0));
    }
}
