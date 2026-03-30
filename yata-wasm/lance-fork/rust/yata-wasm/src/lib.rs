use wasm_bindgen::prelude::*;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use bytes::{BufMut, Bytes, BytesMut};
use lance_core::datatypes::Schema as LanceSchema;
use lance_encoding::decoder::PageEncoding;
use lance_encoding::encoder::{
    default_encoding_strategy, encode_batch, EncodedBatch, EncodingOptions,
};
use lance_encoding::version::LanceFileVersion;
use prost::Message;
use prost_types::Any;
use std::sync::Arc;

mod pbfile {
    #![allow(clippy::all, non_camel_case_types, unused)]
    include!(concat!(env!("OUT_DIR"), "/lance.file.v2.rs"));
}

/// Lance v2 file magic bytes
const MAGIC: &[u8; 4] = b"LANC";

#[wasm_bindgen]
pub fn probe() -> String {
    "lance-core + lance-encoding + lance-file-assembly wasm ok".to_string()
}

/// Encode a RecordBatch to a complete Lance v2 file (in memory).
/// Returns the full file bytes including data pages, column metadata, and footer.
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
        Arc::new(lance_schema.clone()),
        strategy.as_ref(),
        &options,
    ))
    .map_err(|e| e.to_string())?;

    // Assemble full Lance v2 file: data + schema + column metadata + footer
    concat_lance_footer(&encoded, &lance_schema)
}

/// Assemble a complete Lance v2 file from EncodedBatch.
/// Logic ported from lance-file/src/v2/writer.rs `concat_lance_footer`.
fn concat_lance_footer(batch: &EncodedBatch, _lance_schema: &LanceSchema) -> Result<Bytes, String> {
    let mut data = BytesMut::with_capacity(batch.data.len() + 1024 * 1024);
    data.put(batch.data.clone());

    // No global buffers (mini lance mode — schema provided externally)
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
                            pbfile::DirectEncoding {
                                encoding: encoded_encoding,
                            },
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
                    pbfile::DirectEncoding {
                        encoding: encoded_col_encoding,
                    },
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

/// Encode vertex data to a complete Lance v2 file.
/// Returns full file bytes ready for R2 PUT.
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

    let file_bytes = encode_to_lance_file(&batch)
        .map_err(|e| JsValue::from_str(&e))?;

    Ok(file_bytes.to_vec())
}

/// Get the Lance file footer from raw file bytes.
/// Returns JSON with version info and metadata offsets.
#[wasm_bindgen]
pub fn read_lance_footer(file_bytes: &[u8]) -> Result<String, JsValue> {
    if file_bytes.len() < 40 {
        return Err(JsValue::from_str("file too small for Lance footer"));
    }

    let footer = &file_bytes[file_bytes.len() - 40..];

    // Check magic
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
    }).to_string())
}
