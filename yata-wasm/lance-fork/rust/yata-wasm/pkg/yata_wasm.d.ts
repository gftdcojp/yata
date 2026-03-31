/* tslint:disable */
/* eslint-disable */
export function probe(): string;
/**
 * Encode vertex data to a Lance v2 file.
 */
export function encode_vertex_lance(labels: string[], pk_values: string[], repos: string[], rkeys: string[], props_jsons: string[]): Uint8Array;
/**
 * Read the Lance v2 footer from raw file bytes.
 */
export function read_lance_footer(file_bytes: Uint8Array): string;
/**
 * Generate a Lance Dataset fragment path (UUID-based, LanceDB standard layout).
 */
export function generate_fragment_path(): string;
/**
 * Create a new manifest (version 1) with a single fragment.
 * Returns serialized protobuf bytes for the manifest.
 */
export function create_manifest(fragment_path: string, num_rows: number, field_names: string[], field_ids: Int32Array): Uint8Array;
/**
 * Add a fragment to an existing manifest. Returns updated manifest file bytes.
 * Increments version, assigns new fragment ID.
 */
export function add_fragment_to_manifest(manifest_file_bytes: Uint8Array, fragment_path: string, num_rows: number, field_ids: Int32Array): Uint8Array;
/**
 * Get the manifest version path (V2 naming scheme: {version:020}.manifest).
 */
export function manifest_path(version: number): string;
/**
 * Parse a manifest and return summary as JSON.
 */
export function read_manifest(manifest_bytes: Uint8Array): string;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly probe: () => [number, number];
  readonly encode_vertex_lance: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number, j: number) => [number, number, number, number];
  readonly read_lance_footer: (a: number, b: number) => [number, number, number, number];
  readonly generate_fragment_path: () => [number, number];
  readonly create_manifest: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => [number, number, number, number];
  readonly add_fragment_to_manifest: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => [number, number, number, number];
  readonly manifest_path: (a: number) => [number, number];
  readonly read_manifest: (a: number, b: number) => [number, number, number, number];
  readonly __wbindgen_exn_store: (a: number) => void;
  readonly __externref_table_alloc: () => number;
  readonly __wbindgen_export_2: WebAssembly.Table;
  readonly __wbindgen_malloc: (a: number, b: number) => number;
  readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
  readonly __wbindgen_free: (a: number, b: number, c: number) => void;
  readonly __externref_table_dealloc: (a: number) => void;
  readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;
/**
* Instantiates the given `module`, which can either be bytes or
* a precompiled `WebAssembly.Module`.
*
* @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
*
* @returns {InitOutput}
*/
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
* If `module_or_path` is {RequestInfo} or {URL}, makes a request and
* for everything else, calls `WebAssembly.instantiate` directly.
*
* @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
*
* @returns {Promise<InitOutput>}
*/
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
