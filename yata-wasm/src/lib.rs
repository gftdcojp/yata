use wasm_bindgen::prelude::*;

/// Minimal probe: can lance-core types be used in WASM?
#[wasm_bindgen]
pub fn probe() -> String {
    "lance-core wasm probe ok".to_string()
}
