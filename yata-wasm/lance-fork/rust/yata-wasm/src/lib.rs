use wasm_bindgen::prelude::*;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use bytes::{BufMut, Bytes, BytesMut};
use lance_core::datatypes::Schema as LanceSchema;
use lance_encoding::decoder::PageEncoding;
use lance_encoding::encoder::{
    default_encoding_strategy, encode_batch, EncodingOptions,
};
use lance_encoding::version::LanceFileVersion;
use prost::Message;
use prost_types::Any;
use std::sync::Arc;

/// Generated protobuf types — nested to match proto package hierarchy
mod lance {
    #![allow(clippy::all, non_camel_case_types, unused)]

    pub mod file {
        include!(concat!(env!("OUT_DIR"), "/lance.file.rs"));

        pub mod v2 {
            include!(concat!(env!("OUT_DIR"), "/lance.file.v2.rs"));
        }
    }

    pub mod table {
        include!(concat!(env!("OUT_DIR"), "/lance.table.rs"));
    }
}

// Convenience aliases
use lance::file as pb;
use lance::file::v2 as pbfile;
use lance::table as pbtable;

/// Lance v2 file magic bytes
const MAGIC: &[u8; 4] = b"LANC";

#[wasm_bindgen]
pub fn probe() -> String {
    "lance-core+encoding+io+file+table wasm ok".to_string()
}

/// Encode a RecordBatch to a complete Lance v2 file (in memory).
fn encode_to_lance_file(batch: &RecordBatch) -> Result<Bytes, String> {
    let schema = batch.schema();
    let lance_schema =
        LanceSchema::try_from(schema.as_ref()).map_err(|e| e.to_string())?;

    let strategy = default_encoding_strategy(LanceFileVersion::V2_1);
    let options = EncodingOptions {
        cache_bytes_per_column: 8 * 1024 * 1024,
        max_page_bytes: 32 * 1024 * 1024,
        keep_original_array: false,
        buffer_alignment: 64,
    };

    let encoded = futures::executor::block_on(encode_batch(
        batch,
        Arc::new(lance_schema),
        strategy.as_ref(),
        &options,
    ))
    .map_err(|e| e.to_string())?;

    concat_lance_footer(&encoded)
}

/// Assemble a complete Lance v2 file from EncodedBatch (mini lance — no schema).
fn concat_lance_footer(
    batch: &lance_encoding::encoder::EncodedBatch,
) -> Result<Bytes, String> {
    let mut data = BytesMut::with_capacity(batch.data.len() + 64 * 1024);
    data.put(batch.data.clone());

    // No global buffers (mini lance mode)
    let global_buffers: Vec<(u64, u64)> = vec![];

    // Write column metadata
    let col_metadata_start = data.len() as u64;
    let mut col_metadata_positions = Vec::new();

    for col in &batch.page_table {
        let position = data.len() as u64;
        let pages = col
            .page_infos
            .iter()
            .map(|page_info| {
                let encoded_encoding = match &page_info.encoding {
                    PageEncoding::Legacy(array_encoding) => {
                        Any::from_msg(array_encoding).map_err(|e| e.to_string())?.encode_to_vec()
                    }
                    PageEncoding::Structural(page_layout) => {
                        Any::from_msg(page_layout).map_err(|e| e.to_string())?.encode_to_vec()
                    }
                };
                let (buffer_offsets, buffer_sizes): (Vec<_>, Vec<_>) = page_info
                    .buffer_offsets_and_sizes
                    .as_ref()
                    .iter()
                    .cloned()
                    .unzip();
                Ok(pbfile::column_metadata::Page {
                    buffer_offsets,
                    buffer_sizes,
                    encoding: Some(pbfile::Encoding {
                        location: Some(pbfile::encoding::Location::Direct(
                            pbfile::DirectEncoding { encoding: encoded_encoding },
                        )),
                    }),
                    length: page_info.num_rows,
                    priority: page_info.priority,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;

        let (buffer_offsets, buffer_sizes): (Vec<_>, Vec<_>) =
            col.buffer_offsets_and_sizes.iter().cloned().unzip();
        let encoded_col_encoding = Any::from_msg(&col.encoding)
            .map_err(|e| e.to_string())?
            .encode_to_vec();

        let column = pbfile::ColumnMetadata {
            pages,
            buffer_offsets,
            buffer_sizes,
            encoding: Some(pbfile::Encoding {
                location: Some(pbfile::encoding::Location::Direct(
                    pbfile::DirectEncoding { encoding: encoded_col_encoding },
                )),
            }),
        };
        let column_bytes = column.encode_to_vec();
        col_metadata_positions.push((position, column_bytes.len() as u64));
        data.put(column_bytes.as_slice());
    }

    // Column metadata offsets table
    let cmo_table_start = data.len() as u64;
    for (meta_pos, meta_len) in col_metadata_positions {
        data.put_u64_le(meta_pos);
        data.put_u64_le(meta_len);
    }

    // Global buffers offsets table
    let gbo_table_start = data.len() as u64;
    let num_global_buffers = global_buffers.len() as u32;
    for (gbo_pos, gbo_len) in global_buffers {
        data.put_u64_le(gbo_pos);
        data.put_u64_le(gbo_len);
    }

    let (major, minor) = LanceFileVersion::V2_1.to_numbers();

    // Footer (40 bytes)
    data.put_u64_le(col_metadata_start);
    data.put_u64_le(cmo_table_start);
    data.put_u64_le(gbo_table_start);
    data.put_u32_le(num_global_buffers);
    data.put_u32_le(batch.page_table.len() as u32);
    data.put_u16_le(major as u16);
    data.put_u16_le(minor as u16);
    data.put(MAGIC.as_slice());

    Ok(data.freeze())
}

// ── wasm-bindgen API ──

/// Encode vertex data to a Lance v2 file.
#[wasm_bindgen]
pub fn encode_vertex_lance(
    labels: Vec<String>,
    pk_values: Vec<String>,
    repos: Vec<String>,
    rkeys: Vec<String>,
    props_jsons: Vec<String>,
) -> Result<Vec<u8>, JsValue> {
    let n = labels.len();
    if pk_values.len() != n || repos.len() != n || rkeys.len() != n || props_jsons.len() != n {
        return Err(JsValue::from_str("all arrays must have same length"));
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("label", DataType::Utf8, false),
        Field::new("pk_value", DataType::Utf8, false),
        Field::new("repo", DataType::Utf8, true),
        Field::new("rkey", DataType::Utf8, true),
        Field::new("props_json", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::StringArray::from(labels)),
            Arc::new(arrow_array::StringArray::from(pk_values)),
            Arc::new(arrow_array::StringArray::from(repos)),
            Arc::new(arrow_array::StringArray::from(rkeys)),
            Arc::new(arrow_array::StringArray::from(props_jsons)),
        ],
    )
    .map_err(|e| JsValue::from_str(&format!("{e}")))?;

    let file_bytes = encode_to_lance_file(&batch).map_err(|e| JsValue::from_str(&e))?;
    Ok(file_bytes.to_vec())
}

/// Read the Lance v2 footer from raw file bytes.
#[wasm_bindgen]
pub fn read_lance_footer(file_bytes: &[u8]) -> Result<String, JsValue> {
    if file_bytes.len() < 40 {
        return Err(JsValue::from_str("file too small for Lance footer"));
    }

    let footer = &file_bytes[file_bytes.len() - 40..];
    if &footer[36..40] != MAGIC {
        return Err(JsValue::from_str("not a Lance file (missing LANC magic)"));
    }

    let col_metadata_start = u64::from_le_bytes(footer[0..8].try_into().unwrap());
    let cmo_table_start = u64::from_le_bytes(footer[8..16].try_into().unwrap());
    let gbo_table_start = u64::from_le_bytes(footer[16..24].try_into().unwrap());
    let num_global_buffers = u32::from_le_bytes(footer[24..28].try_into().unwrap());
    let num_columns = u32::from_le_bytes(footer[28..32].try_into().unwrap());
    let major = u16::from_le_bytes(footer[32..34].try_into().unwrap());
    let minor = u16::from_le_bytes(footer[34..36].try_into().unwrap());

    Ok(serde_json::json!({
        "col_metadata_start": col_metadata_start,
        "cmo_table_start": cmo_table_start,
        "gbo_table_start": gbo_table_start,
        "num_global_buffers": num_global_buffers,
        "num_columns": num_columns,
        "major_version": major,
        "minor_version": minor,
        "file_size": file_bytes.len(),
    })
    .to_string())
}

/// Generate a Lance Dataset fragment path (UUID-based, LanceDB standard layout).
#[wasm_bindgen]
pub fn generate_fragment_path() -> String {
    let uuid = uuid::Uuid::new_v4();
    format!("data/{}.lance", uuid)
}

// ── Manifest Operations ──

/// Create a new manifest (version 1) with a single fragment.
/// Returns serialized protobuf bytes for the manifest.
#[wasm_bindgen]
pub fn create_manifest(
    fragment_path: &str,
    num_rows: u32,
    field_names: Vec<String>,
    field_ids: Vec<i32>,
) -> Result<Vec<u8>, JsValue> {
    use prost::Message;

    let fields: Vec<pb::Field> = field_names
        .iter()
        .zip(field_ids.iter())
        .enumerate()
        .map(|(_, (name, &id))| pb::Field {
            name: name.clone(),
            id,
            parent_id: -1,
            logical_type: "utf8".to_string(),
            ..Default::default()
        })
        .collect();

    let data_file = pbtable::DataFile {
        path: fragment_path.to_string(),
        fields: field_ids.clone(),
        ..Default::default()
    };

    let fragment = pbtable::DataFragment {
        id: 0,
        files: vec![data_file],
        physical_rows: num_rows as u64,
        ..Default::default()
    };

    let manifest = pbtable::Manifest {
        fields,
        fragments: vec![fragment],
        version: 1,
        max_fragment_id: 0,
        writer_version: Some(pbtable::manifest::WriterVersion {
            library: "yata-wasm".to_string(),
            version: "0.1.0".to_string(),
        }),
        data_format: Some(pbtable::manifest::DataStorageFormat {
            file_format: "lance".to_string(),
            version: "2.1".to_string(),
        }),
        ..Default::default()
    };

    let bytes = manifest.encode_to_vec();
    Ok(bytes)
}

/// Add a fragment to an existing manifest. Returns updated manifest bytes.
/// Increments version, assigns new fragment ID.
#[wasm_bindgen]
pub fn add_fragment_to_manifest(
    manifest_bytes: &[u8],
    fragment_path: &str,
    num_rows: u32,
    field_ids: Vec<i32>,
) -> Result<Vec<u8>, JsValue> {
    use prost::Message;

    let mut manifest = pbtable::Manifest::decode(manifest_bytes)
        .map_err(|e| JsValue::from_str(&format!("manifest decode error: {e}")))?;

    let new_fragment_id = manifest.max_fragment_id + 1;
    manifest.max_fragment_id = new_fragment_id;
    manifest.version += 1;

    let data_file = pbtable::DataFile {
        path: fragment_path.to_string(),
        fields: field_ids,
        ..Default::default()
    };

    manifest.fragments.push(pbtable::DataFragment {
        id: new_fragment_id as u64,
        files: vec![data_file],
        physical_rows: num_rows as u64,
        ..Default::default()
    });

    Ok(manifest.encode_to_vec())
}

/// Get the manifest version path (V2 naming scheme: {version:020}.manifest).
#[wasm_bindgen]
pub fn manifest_path(version: u32) -> String {
    format!("_versions/{:020}.manifest", version)
}

/// Parse a manifest and return summary as JSON.
#[wasm_bindgen]
pub fn read_manifest(manifest_bytes: &[u8]) -> Result<String, JsValue> {
    use prost::Message;

    let manifest = pbtable::Manifest::decode(manifest_bytes)
        .map_err(|e| JsValue::from_str(&format!("manifest decode error: {e}")))?;

    let fragments: Vec<serde_json::Value> = manifest
        .fragments
        .iter()
        .map(|f| {
            serde_json::json!({
                "id": f.id,
                "physical_rows": f.physical_rows,
                "files": f.files.iter().map(|df| &df.path).collect::<Vec<_>>(),
            })
        })
        .collect();

    Ok(serde_json::json!({
        "version": manifest.version,
        "max_fragment_id": manifest.max_fragment_id,
        "num_fragments": manifest.fragments.len(),
        "fragments": fragments,
        "num_fields": manifest.fields.len(),
    })
    .to_string())
}
