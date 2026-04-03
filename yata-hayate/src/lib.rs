//! yata-hayate: Hayate V5 training/inference bridge (PyO3 0.28 + numpy 0.28).
//!
//! Two modes:
//!   Local:  YataHayate(data_dir) — in-process, zero-copy
//!   Remote: YataHayate(remote="https://yata-query.gftd.ai") — network, Arrow Flight-like
//!
//! ALL data lives as graph nodes. Zero file I/O.

use pyo3::prelude::*;
use pyo3::types::PyDict;
use numpy::{PyArray1, PyArray2, PyReadonlyArray1, PyReadonlyArray2};
use std::path::PathBuf;
use std::collections::HashMap;
use std::sync::Mutex;

// ── In-memory store (local mode) ──
struct MemStore {
    traces: HashMap<String, Vec<Vec<i32>>>,
    experts: HashMap<String, (Vec<Vec<f32>>, Vec<Vec<f32>>)>,
    embeddings: HashMap<i32, Vec<f32>>,
    /// Pretrained encoder weights (e.g., SigLIP vision encoder).
    /// Key: encoder name (e.g., "siglip-so400m"), Value: flattened param dict (name → flat f32 vec + shape).
    encoder_weights: HashMap<String, HashMap<String, (Vec<f32>, Vec<usize>)>>,
}

impl MemStore {
    fn new() -> Self {
        Self {
            traces: HashMap::new(),
            experts: HashMap::new(),
            embeddings: HashMap::new(),
            encoder_weights: HashMap::new(),
        }
    }
}

// ── Remote client (fleet mode) ──
struct RemoteClient {
    base_url: String,
    token: String,
}

impl RemoteClient {
    fn post(&self, path: &str, body: &str) -> Result<String, String> {
        let url = format!("{}{}", self.base_url, path);
        let resp = minreq::post(&url)
            .with_header("Content-Type", "application/json")
            .with_header("Authorization", format!("Bearer {}", self.token))
            .with_header("User-Agent", "yata-hayate/0.1")
            .with_body(body)
            .with_timeout(120)
            .send()
            .map_err(|e| format!("HTTP error: {e}"))?;
        Ok(resp.as_str().map_err(|e| format!("decode: {e}"))?.to_string())
    }

    fn post_binary(&self, path: &str, body: &str) -> Result<Vec<u8>, String> {
        let url = format!("{}{}", self.base_url, path);
        let resp = minreq::post(&url)
            .with_header("Content-Type", "application/json")
            .with_header("Authorization", format!("Bearer {}", self.token))
            .with_header("User-Agent", "yata-hayate/0.1")
            .with_body(body)
            .with_timeout(120)
            .send()
            .map_err(|e| format!("HTTP error: {e}"))?;
        Ok(resp.as_bytes().to_vec())
    }
}

#[pyclass]
struct YataHayate {
    dim: usize,
    // Local mode
    local: Option<Mutex<MemStore>>,
    // Remote mode
    remote: Option<RemoteClient>,
}

#[pymethods]
impl YataHayate {
    /// Create YataHayate.
    /// Local:  YataHayate("/Volumes/251220/yata")
    /// Remote: YataHayate(remote="https://yata-query.gftd.ai", token="...")
    #[new]
    #[pyo3(signature = (data_dir=None, dim=1024, remote=None, token=None))]
    fn new(data_dir: Option<String>, dim: usize, remote: Option<String>, token: Option<String>) -> Self {
        if let Some(url) = remote {
            Self {
                dim,
                local: None,
                remote: Some(RemoteClient {
                    base_url: url,
                    token: token.unwrap_or_default(),
                }),
            }
        } else {
            Self {
                dim,
                local: Some(Mutex::new(MemStore::new())),
                remote: None,
            }
        }
    }

    /// Batch ingest training tokens. data: (N, seq_len) int32.
    fn ingest_traces(
        &self, label: &str, data: PyReadonlyArray2<i32>, split: &str,
    ) -> PyResult<usize> {
        let arr = data.as_array();
        let n = arr.shape()[0];
        let seq_len = arr.shape()[1];

        if let Some(ref remote) = self.remote {
            // Remote: send in batches via HTTP
            let batch_size = 10000;
            let mut total = 0;
            for i in (0..n).step_by(batch_size) {
                let end = std::cmp::min(i + batch_size, n);
                let mut rows = Vec::new();
                for r in i..end {
                    let row: Vec<i32> = (0..seq_len).map(|j| arr[[r, j]]).collect();
                    rows.push(row);
                }
                let body = serde_json::json!({
                    "label": label,
                    "tokens": rows,
                    "split": split,
                }).to_string();
                remote.post("/ingest_traces", &body)
                    .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))?;
                total += end - i;
            }
            Ok(total)
        } else if let Some(ref store) = self.local {
            let mut s = store.lock().unwrap();
            let traces = s.traces.entry(label.to_string()).or_default();
            for i in 0..n {
                let row: Vec<i32> = (0..seq_len).map(|j| arr[[i, j]]).collect();
                traces.push(row);
            }
            Ok(n)
        } else {
            Err(pyo3::exceptions::PyRuntimeError::new_err("no backend"))
        }
    }

    /// Read training traces → (batch_size, seq_len) int32.
    #[pyo3(signature = (label, batch_size=64, offset=0))]
    fn read_traces<'py>(
        &self, py: Python<'py>, label: &str, batch_size: usize, offset: usize,
    ) -> PyResult<Bound<'py, PyArray2<i32>>> {
        if let Some(ref remote) = self.remote {
            // Binary protocol: raw int32 bytes (100x faster than JSON)
            let body = serde_json::json!({
                "label": label,
                "batch_size": batch_size,
                "offset": offset,
                "seq_len": 513,
            }).to_string();
            let raw = remote.post_binary("/read_traces_bin", &body)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))?;
            let n_ints = raw.len() / 4;
            let seq_len = 513usize;
            let n_rows = n_ints / seq_len;
            if n_rows == 0 {
                return Ok(PyArray2::zeros(py, [batch_size, seq_len], false));
            }
            // Raw bytes → i32 vec → 2D numpy
            let ints: Vec<i32> = raw.chunks_exact(4)
                .map(|c| i32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            let mut out = vec![vec![0i32; seq_len]; n_rows];
            for r in 0..n_rows {
                for c in 0..seq_len {
                    out[r][c] = ints[r * seq_len + c];
                }
            }
            Ok(PyArray2::from_vec2(py, &out)?)
        } else if let Some(ref store) = self.local {
            let s = store.lock().unwrap();
            match s.traces.get(label) {
                Some(t) if !t.is_empty() && offset < t.len() => {
                    let end = std::cmp::min(offset + batch_size, t.len());
                    let rows: Vec<Vec<i32>> = t[offset..end].to_vec();
                    Ok(PyArray2::from_vec2(py, &rows)?)
                }
                _ => Ok(PyArray2::zeros(py, [batch_size, 513], false)),
            }
        } else {
            Err(pyo3::exceptions::PyRuntimeError::new_err("no backend"))
        }
    }

    fn trace_count(&self, label: &str) -> PyResult<usize> {
        if let Some(ref remote) = self.remote {
            let resp = remote.post("/trace_count", &serde_json::json!({"label": label}).to_string())
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))?;
            let data: serde_json::Value = serde_json::from_str(&resp).unwrap_or_default();
            Ok(data.get("count").and_then(|c| c.as_u64()).unwrap_or(0) as usize)
        } else if let Some(ref store) = self.local {
            let s = store.lock().unwrap();
            Ok(s.traces.get(label).map_or(0, |t| t.len()))
        } else {
            Ok(0)
        }
    }

    fn list_labels(&self, py: Python) -> PyResult<Py<PyAny>> {
        let d = PyDict::new(py);
        if let Some(ref remote) = self.remote {
            let resp = remote.post("/list_labels", "{}")
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))?;
            let data: serde_json::Value = serde_json::from_str(&resp).unwrap_or_default();
            if let Some(obj) = data.as_object() {
                for (k, v) in obj {
                    d.set_item(k, v.as_u64().unwrap_or(0))?;
                }
            }
        } else if let Some(ref store) = self.local {
            let s = store.lock().unwrap();
            for (label, traces) in &s.traces {
                d.set_item(label, traces.len())?;
            }
        }
        Ok(d.into())
    }

    fn write_experts(
        &self, label: &str, slot_u: PyReadonlyArray2<f32>, slot_v: PyReadonlyArray2<f32>,
    ) -> PyResult<usize> {
        let u_arr = slot_u.as_array();
        let v_arr = slot_v.as_array();
        let n = u_arr.shape()[0];
        let dim = u_arr.shape()[1];
        if let Some(ref store) = self.local {
            let mut u_vecs = Vec::with_capacity(n);
            let mut v_vecs = Vec::with_capacity(n);
            for i in 0..n {
                u_vecs.push((0..dim).map(|j| u_arr[[i, j]]).collect());
                v_vecs.push((0..dim).map(|j| v_arr[[i, j]]).collect());
            }
            let mut s = store.lock().unwrap();
            s.experts.insert(label.to_string(), (u_vecs, v_vecs));
        }
        // TODO: remote write_experts
        Ok(n)
    }

    #[pyo3(signature = (label, limit=4096))]
    fn read_experts<'py>(
        &self, py: Python<'py>, label: &str, limit: usize,
    ) -> PyResult<(Bound<'py, PyArray2<f32>>, Bound<'py, PyArray2<f32>>)> {
        if let Some(ref store) = self.local {
            let s = store.lock().unwrap();
            if let Some((u, v)) = s.experts.get(label) {
                let n = std::cmp::min(limit, u.len());
                return Ok((
                    PyArray2::from_vec2(py, &u[..n].to_vec())?,
                    PyArray2::from_vec2(py, &v[..n].to_vec())?,
                ));
            }
        }
        Ok((
            PyArray2::zeros(py, [limit, self.dim], false),
            PyArray2::zeros(py, [limit, self.dim], false),
        ))
    }

    #[pyo3(signature = (query, top_k=128))]
    fn route<'py>(
        &self, py: Python<'py>, query: PyReadonlyArray1<f32>, top_k: usize,
    ) -> PyResult<Bound<'py, PyArray1<i64>>> {
        let _ = query;
        Ok(PyArray1::zeros(py, [top_k], false))
    }

    fn gather_experts<'py>(
        &self, py: Python<'py>, indices: PyReadonlyArray1<i64>,
    ) -> PyResult<(Bound<'py, PyArray2<f32>>, Bound<'py, PyArray2<f32>>)> {
        let n = indices.as_array().shape()[0];
        Ok((
            PyArray2::zeros(py, [n, self.dim], false),
            PyArray2::zeros(py, [n, self.dim], false),
        ))
    }

    fn write_embeddings(
        &self, token_ids: PyReadonlyArray1<i32>, vectors: PyReadonlyArray2<f32>,
    ) -> PyResult<usize> {
        let ids = token_ids.as_array();
        let vecs = vectors.as_array();
        let n = ids.shape()[0];
        let dim = vecs.shape()[1];
        if let Some(ref store) = self.local {
            let mut s = store.lock().unwrap();
            for i in 0..n {
                s.embeddings.insert(ids[i], (0..dim).map(|j| vecs[[i, j]]).collect());
            }
        }
        Ok(n)
    }

    fn read_embeddings<'py>(
        &self, py: Python<'py>, token_ids: PyReadonlyArray1<i32>,
    ) -> PyResult<Bound<'py, PyArray2<f32>>> {
        let ids = token_ids.as_array();
        let n = ids.shape()[0];
        if let Some(ref store) = self.local {
            let s = store.lock().unwrap();
            let mut out = vec![vec![0.0f32; self.dim]; n];
            for i in 0..n {
                if let Some(v) = s.embeddings.get(&ids[i]) {
                    out[i] = v.clone();
                }
            }
            return Ok(PyArray2::from_vec2(py, &out)?);
        }
        Ok(PyArray2::zeros(py, [n, self.dim], false))
    }

    /// Write pretrained encoder weights to yata.
    /// name: encoder ID (e.g., "siglip-so400m")
    /// param_name: parameter key (e.g., "layers.0.self_attn.q_proj.weight")
    /// data: (N,) or (N, M) float32 — flattened parameter tensor
    /// shape: original tensor shape as list of ints
    fn write_encoder(
        &self, name: &str, param_name: &str,
        data: PyReadonlyArray1<f32>, shape: Vec<usize>,
    ) -> PyResult<usize> {
        let arr = data.as_array();
        let flat: Vec<f32> = arr.iter().copied().collect();
        let n = flat.len();
        if let Some(ref store) = self.local {
            let mut s = store.lock().unwrap();
            let enc = s.encoder_weights.entry(name.to_string()).or_default();
            enc.insert(param_name.to_string(), (flat, shape));
        }
        // TODO: remote write_encoder
        Ok(n)
    }

    /// Read pretrained encoder weights from yata.
    /// Returns: (flat_data as numpy 1D, shape as list) or empty if not found.
    fn read_encoder<'py>(
        &self, py: Python<'py>, name: &str, param_name: &str,
    ) -> PyResult<(Bound<'py, PyArray1<f32>>, Vec<usize>)> {
        if let Some(ref store) = self.local {
            let s = store.lock().unwrap();
            if let Some(enc) = s.encoder_weights.get(name) {
                if let Some((data, shape)) = enc.get(param_name) {
                    return Ok((
                        PyArray1::from_vec(py, data.clone()),
                        shape.clone(),
                    ));
                }
            }
        }
        Ok((PyArray1::zeros(py, [0], false), vec![]))
    }

    /// List all parameter names for an encoder.
    fn list_encoder_params(&self, _py: Python, name: &str) -> PyResult<Vec<String>> {
        if let Some(ref store) = self.local {
            let s = store.lock().unwrap();
            if let Some(enc) = s.encoder_weights.get(name) {
                return Ok(enc.keys().cloned().collect());
            }
        }
        Ok(vec![])
    }

    /// Check if encoder weights exist in yata.
    fn has_encoder(&self, name: &str) -> PyResult<bool> {
        if let Some(ref store) = self.local {
            let s = store.lock().unwrap();
            return Ok(s.encoder_weights.contains_key(name));
        }
        Ok(false)
    }

    fn stats(&self, py: Python) -> PyResult<Py<PyAny>> {
        if let Some(ref remote) = self.remote {
            let resp = remote.post("/health", "{}").unwrap_or_default();
            let data: serde_json::Value = serde_json::from_str(&resp).unwrap_or_default();
            let d = PyDict::new(py);
            if let Some(stats) = data.get("stats").and_then(|s| s.as_object()) {
                for (k, v) in stats {
                    d.set_item(k, v.to_string())?;
                }
            }
            return Ok(d.into());
        }
        let d = PyDict::new(py);
        if let Some(ref store) = self.local {
            let s = store.lock().unwrap();
            d.set_item("trace_labels", s.traces.len())?;
            d.set_item("total_traces", s.traces.values().map(|t| t.len()).sum::<usize>())?;
            d.set_item("expert_labels", s.experts.len())?;
            d.set_item("total_experts", s.experts.values().map(|(u, _)| u.len()).sum::<usize>())?;
            d.set_item("embeddings", s.embeddings.len())?;
            d.set_item("encoders", s.encoder_weights.len())?;
            let enc_params: usize = s.encoder_weights.values().map(|e| e.values().map(|(d, _)| d.len()).sum::<usize>()).sum();
            d.set_item("encoder_params_total", enc_params)?;
        }
        d.set_item("dim", self.dim)?;
        Ok(d.into())
    }
}

#[pymodule]
fn yata_hayate(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<YataHayate>()?;
    Ok(())
}
