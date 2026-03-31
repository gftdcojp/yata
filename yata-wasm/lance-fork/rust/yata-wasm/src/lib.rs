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

/// Decode a Lance v2 file → JSON rows.
#[wasm_bindgen]
pub fn decode_lance_fragment(file_bytes: &[u8]) -> Result<String, JsValue> {
    use lance_encoding::decoder::{
        decode_batch as lance_decode, ColumnInfo as DecColumnInfo, PageInfo as DecPageInfo,
        DecoderPlugins, FilterExpression, PageEncoding,
    };
    use lance_encoding::format::pb as enc_pb;
    use lance_core::cache::{CapacityMode, FileMetadataCache};

    if file_bytes.len() < 40 {
        return Err(JsValue::from_str("file too small"));
    }

    // 1. Parse footer (last 40 bytes)
    let footer = &file_bytes[file_bytes.len() - 40..];
    if &footer[36..40] != MAGIC {
        return Err(JsValue::from_str("not a Lance file"));
    }
    let col_metadata_start = u64::from_le_bytes(footer[0..8].try_into().unwrap()) as usize;
    let cmo_table_start = u64::from_le_bytes(footer[8..16].try_into().unwrap()) as usize;
    let gbo_table_start = u64::from_le_bytes(footer[16..24].try_into().unwrap()) as usize;
    let num_global_buffers = u32::from_le_bytes(footer[24..28].try_into().unwrap()) as usize;
    let num_columns = u32::from_le_bytes(footer[28..32].try_into().unwrap()) as usize;

    // 2. Read schema from global buffer (first global buffer = FileDescriptor)
    let schema = if num_global_buffers > 0 {
        let gbo_pos = u64::from_le_bytes(file_bytes[gbo_table_start..gbo_table_start + 8].try_into().unwrap()) as usize;
        let gbo_len = u64::from_le_bytes(file_bytes[gbo_table_start + 8..gbo_table_start + 16].try_into().unwrap()) as usize;
        let descriptor = pb::FileDescriptor::decode(&file_bytes[gbo_pos..gbo_pos + gbo_len])
            .map_err(|e| JsValue::from_str(&format!("descriptor: {e}")))?;
        if let Some(schema_pb) = descriptor.schema {
            let fields: Vec<arrow_schema::Field> = schema_pb.fields.iter().map(|f| {
                let dt = lance_core::datatypes::LogicalType::from(f.logical_type.as_str());
                let arrow_dt = arrow_schema::DataType::try_from(&dt).unwrap_or(arrow_schema::DataType::Utf8);
                arrow_schema::Field::new(&f.name, arrow_dt, f.nullable)
            }).collect();
            Arc::new(arrow_schema::Schema::new(fields))
        } else {
            return Err(JsValue::from_str("no schema in file descriptor"));
        }
    } else {
        return Err(JsValue::from_str("no global buffers (schema missing)"));
    };

    let lance_schema = Arc::new(
        LanceSchema::try_from(schema.as_ref()).map_err(|e| JsValue::from_str(&e.to_string()))?
    );

    // 3. Parse column metadata → build page_table
    let data = Bytes::copy_from_slice(&file_bytes[..col_metadata_start]);
    let mut page_table: Vec<Arc<DecColumnInfo>> = Vec::new();
    let mut total_rows = 0u64;

    for col_idx in 0..num_columns {
        let off = cmo_table_start + col_idx * 16;
        let meta_pos = u64::from_le_bytes(file_bytes[off..off + 8].try_into().unwrap()) as usize;
        let meta_len = u64::from_le_bytes(file_bytes[off + 8..off + 16].try_into().unwrap()) as usize;
        let col_meta = pbfile::ColumnMetadata::decode(&file_bytes[meta_pos..meta_pos + meta_len])
            .map_err(|e| JsValue::from_str(&format!("col meta {col_idx}: {e}")))?;

        let mut page_infos = Vec::new();
        for page in &col_meta.pages {
            let buf_off_sizes: Vec<(u64, u64)> = page.buffer_offsets.iter()
                .zip(page.buffer_sizes.iter())
                .map(|(&o, &s)| (o, s))
                .collect();

            let encoding = if let Some(enc) = &page.encoding {
                if let Some(pbfile::encoding::Location::Direct(direct)) = &enc.location {
                    // direct.encoding is Any-wrapped protobuf: decode Any first, then inner message
                    let any = Any::decode(direct.encoding.as_slice())
                        .map_err(|e| JsValue::from_str(&format!("Any decode: {e}")))?;
                    if any.type_url.contains("PageLayout") {
                        let layout = enc_pb::PageLayout::decode(any.value.as_slice())
                            .map_err(|e| JsValue::from_str(&format!("PageLayout: {e}")))?;
                        PageEncoding::Structural(layout)
                    } else if any.type_url.contains("ArrayEncoding") {
                        let arr = enc_pb::ArrayEncoding::decode(any.value.as_slice())
                            .map_err(|e| JsValue::from_str(&format!("ArrayEncoding: {e}")))?;
                        PageEncoding::Legacy(arr)
                    } else {
                        return Err(JsValue::from_str(&format!("unknown encoding type_url: {}", any.type_url)));
                    }
                } else {
                    return Err(JsValue::from_str("deferred encoding unsupported"));
                }
            } else {
                return Err(JsValue::from_str("missing page encoding"));
            };

            if col_idx == 0 { total_rows += page.length; }
            page_infos.push(DecPageInfo {
                num_rows: page.length,
                priority: page.priority,
                encoding,
                buffer_offsets_and_sizes: Arc::from(buf_off_sizes),
            });
        }

        let col_buf_off_sizes: Vec<(u64, u64)> = col_meta.buffer_offsets.iter()
            .zip(col_meta.buffer_sizes.iter())
            .map(|(&o, &s)| (o, s))
            .collect();

        let col_encoding = if let Some(enc) = &col_meta.encoding {
            if let Some(pbfile::encoding::Location::Direct(direct)) = &enc.location {
                // Column encoding is also Any-wrapped
                if let Ok(any) = Any::decode(direct.encoding.as_slice()) {
                    enc_pb::ColumnEncoding::decode(any.value.as_slice()).unwrap_or_default()
                } else {
                    enc_pb::ColumnEncoding::decode(direct.encoding.as_slice()).unwrap_or_default()
                }
            } else { enc_pb::ColumnEncoding::default() }
        } else { enc_pb::ColumnEncoding::default() };

        page_table.push(Arc::new(DecColumnInfo::new(
            col_idx as u32,
            Arc::from(page_infos),
            col_buf_off_sizes,
            col_encoding,
        )));
    }

    // 4. Build EncodedBatch and decode
    let encoded = lance_encoding::encoder::EncodedBatch {
        data,
        page_table,
        schema: lance_schema,
        top_level_columns: (0..num_columns as u32).collect(),
        num_rows: total_rows,
    };

    let filter = FilterExpression::no_filter();
    let plugins = Arc::new(DecoderPlugins::default());
    let cache = Arc::new(FileMetadataCache::with_capacity(1024 * 1024, CapacityMode::Bytes));

    let batch = futures::executor::block_on(lance_decode(
        &encoded, &filter, plugins, false, LanceFileVersion::V2_1, Some(cache),
    )).map_err(|e| JsValue::from_str(&format!("decode: {e}")))?;

    // 5. Convert RecordBatch → JSON rows
    let mut rows = Vec::new();
    for i in 0..batch.num_rows() {
        let mut row = serde_json::Map::new();
        for (ci, field) in batch.schema().fields().iter().enumerate() {
            let col = batch.column(ci);
            if let Some(arr) = col.as_any().downcast_ref::<arrow_array::StringArray>() {
                use arrow_array::Array;
                row.insert(field.name().clone(), if arr.is_null(i) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(arr.value(i).to_string())
                });
            }
        }
        rows.push(serde_json::Value::Object(row));
    }

    Ok(serde_json::to_string(&rows).unwrap())
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

    // Write global buffer: FileDescriptor with schema (required by V2 reader)
    // Align to 64 bytes (V2.1 requirement)
    let misalign = data.len() % 64;
    if misalign != 0 {
        let padding = 64 - misalign;
        data.extend_from_slice(&vec![0u8; padding]);
    }
    let schema_start = data.len() as u64;
    let lance_schema = LanceSchema::try_from(batch.schema.as_ref()).map_err(|e| e.to_string())?;
    // Convert schema fields to protobuf
    let pb_fields: Vec<pb::Field> = lance_schema.fields.iter().enumerate().map(|(i, f)| {
        pb::Field {
            name: f.name.clone(),
            id: i as i32,
            parent_id: -1,
            logical_type: lance_core::datatypes::LogicalType::try_from(&f.data_type())
                .map(|lt| lt.to_string())
                .unwrap_or_else(|_| "string".to_string()),
            ..Default::default()
        }
    }).collect();
    let descriptor = pb::FileDescriptor {
        schema: Some(pb::Schema {
            fields: pb_fields,
            metadata: Default::default(),
        }),
        length: batch.num_rows,
    };
    let descriptor_bytes = descriptor.encode_to_vec();
    let descriptor_len = descriptor_bytes.len() as u64;
    data.put(descriptor_bytes.as_slice());
    let global_buffers = vec![(schema_start, descriptor_len)];

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
    format!("{}.lance", uuid)
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
            logical_type: "string".to_string(),
            ..Default::default()
        })
        .collect();

    let data_file = pbtable::DataFile {
        path: fragment_path.to_string(),
        fields: field_ids.clone(),
        column_indices: (0..field_ids.len() as i32).collect(),
        file_major_version: 2,
        file_minor_version: 1,
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

    let file_bytes = wrap_manifest_file(&manifest);
    Ok(file_bytes)
}

/// Add a fragment to an existing manifest. Returns updated manifest file bytes.
/// Increments version, assigns new fragment ID.
#[wasm_bindgen]
pub fn add_fragment_to_manifest(
    manifest_file_bytes: &[u8],
    fragment_path: &str,
    num_rows: u32,
    field_ids: Vec<i32>,
) -> Result<Vec<u8>, JsValue> {
    use prost::Message;

    // Parse manifest from file format (skip to protobuf after length prefix)
    let mut manifest = parse_manifest_file(manifest_file_bytes)
        .map_err(|e| JsValue::from_str(&e))?;

    let new_fragment_id = manifest.max_fragment_id + 1;
    manifest.max_fragment_id = new_fragment_id;
    manifest.version += 1;

    let data_file = pbtable::DataFile {
        path: fragment_path.to_string(),
        fields: field_ids.clone(),
        column_indices: (0..field_ids.len() as i32).collect(),
        file_major_version: 2,
        file_minor_version: 1,
        ..Default::default()
    };

    manifest.fragments.push(pbtable::DataFragment {
        id: new_fragment_id as u64,
        files: vec![data_file],
        physical_rows: num_rows as u64,
        ..Default::default()
    });

    Ok(wrap_manifest_file(&manifest))
}

/// Wrap a protobuf Manifest into the Lance manifest file format:
/// [u32: len] [protobuf bytes] [i64: position] [i16: major=0] [i16: minor=1] [b"LANC"]
fn wrap_manifest_file(manifest: &pbtable::Manifest) -> Vec<u8> {
    use prost::Message;
    let proto_bytes = manifest.encode_to_vec();
    let proto_len = proto_bytes.len() as u32;
    let manifest_pos = 0i64; // position of the u32 length prefix

    let mut buf = Vec::with_capacity(proto_bytes.len() + 4 + 16);
    buf.extend_from_slice(&proto_len.to_le_bytes());  // u32 length
    buf.extend_from_slice(&proto_bytes);               // protobuf
    buf.extend_from_slice(&manifest_pos.to_le_bytes()); // i64 position
    buf.extend_from_slice(&0i16.to_le_bytes());        // major version
    buf.extend_from_slice(&1i16.to_le_bytes());        // minor version
    buf.extend_from_slice(b"LANC");                    // magic
    buf
}

/// Parse a Lance manifest file: read footer to find protobuf, then decode.
fn parse_manifest_file(file_bytes: &[u8]) -> Result<pbtable::Manifest, String> {
    use prost::Message;

    if file_bytes.len() < 16 {
        // Might be raw protobuf (old format without file wrapper)
        return pbtable::Manifest::decode(file_bytes)
            .map_err(|e| format!("manifest decode: {e}"));
    }

    // Check for LANC magic at end
    let footer_start = file_bytes.len() - 16;
    let magic = &file_bytes[file_bytes.len() - 4..];
    if magic == b"LANC" {
        // New format: read footer
        let pos = i64::from_le_bytes(file_bytes[footer_start..footer_start + 8].try_into().unwrap()) as usize;
        let proto_len = u32::from_le_bytes(file_bytes[pos..pos + 4].try_into().unwrap()) as usize;
        let proto_start = pos + 4;
        let proto_end = proto_start + proto_len;
        if proto_end > file_bytes.len() {
            return Err("manifest protobuf extends beyond file".to_string());
        }
        pbtable::Manifest::decode(&file_bytes[proto_start..proto_end])
            .map_err(|e| format!("manifest decode: {e}"))
    } else {
        // Fallback: raw protobuf
        pbtable::Manifest::decode(file_bytes)
            .map_err(|e| format!("manifest decode (raw): {e}"))
    }
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

    let manifest = parse_manifest_file(manifest_bytes)
        .map_err(|e| JsValue::from_str(&e))?;

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
