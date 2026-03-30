import {
  Bool,
  Field,
  Schema,
  Table,
  Uint32,
  Uint64,
  Utf8,
  Struct,
  vectorFromArray,
  makeData,
  tableToIPC,
} from "apache-arrow";

export const VERTEX_SCHEMA = new Schema([
  new Field("partition_id", new Uint32(), false),
  new Field("label", new Utf8(), false),
  new Field("pk_key", new Utf8(), false),
  new Field("pk_value", new Utf8(), false),
  new Field("vid", new Utf8(), false),
  new Field("alive", new Bool(), false),
  new Field("latest_seq", new Uint64(), false),
  new Field("repo", new Utf8(), true),
  new Field("rkey", new Utf8(), true),
  new Field("updated_at_ms", new Uint64(), false),
  new Field("props_json", new Utf8(), true),
]);

export const EDGE_SCHEMA = new Schema([
  new Field("partition_id", new Uint32(), false),
  new Field("edge_label", new Utf8(), false),
  new Field("pk_key", new Utf8(), false),
  new Field("pk_value", new Utf8(), false),
  new Field("eid", new Utf8(), false),
  new Field("src_vid", new Utf8(), false),
  new Field("dst_vid", new Utf8(), false),
  new Field("alive", new Bool(), false),
  new Field("latest_seq", new Uint64(), false),
  new Field("updated_at_ms", new Uint64(), false),
  new Field("props_json", new Utf8(), true),
]);

function buildTable(schema: Schema, columns: Record<string, unknown[]>): Table {
  const len = Object.values(columns)[0]?.length ?? 0;
  const children = schema.fields.map(f => {
    const vals = columns[f.name] ?? [];
    return vectorFromArray(vals as any, f.type).data[0];
  });
  const structData = makeData({
    type: new Struct(schema.fields),
    length: len,
    children,
  });
  return new Table(structData);
}

export function makeVertexTable(rows: Record<string, unknown>[]): Table {
  return buildTable(VERTEX_SCHEMA, {
    partition_id: rows.map(() => 0),
    label: rows.map(r => String(r.label ?? "")),
    pk_key: rows.map(r => String(r.pk_key ?? "rkey")),
    pk_value: rows.map(r => String(r.pk_value ?? "")),
    vid: rows.map(r => String(r.vid ?? "")),
    alive: rows.map(r => r.alive !== false),
    latest_seq: rows.map(r => BigInt(r.latest_seq ?? 0)),
    repo: rows.map(r => String(r.repo ?? "")),
    rkey: rows.map(r => String(r.rkey ?? "")),
    updated_at_ms: rows.map(r => BigInt(r.updated_at_ms ?? 0)),
    props_json: rows.map(r => String(r.props_json ?? "{}")),
  });
}

export function makeEdgeTable(
  rows: { src_vid: string; dst_vid: string; alive?: boolean }[],
): Table {
  return buildTable(EDGE_SCHEMA, {
    partition_id: rows.map(() => 0),
    edge_label: rows.map(() => "FOLLOW"),
    pk_key: rows.map((_, i) => `pk_${i}`),
    pk_value: rows.map((_, i) => `pv_${i}`),
    eid: rows.map((_, i) => `e_${i}`),
    src_vid: rows.map(r => r.src_vid),
    dst_vid: rows.map(r => r.dst_vid),
    alive: rows.map(r => r.alive !== false),
    latest_seq: rows.map(() => 1n),
    updated_at_ms: rows.map(() => 1000n),
    props_json: rows.map(() => "{}"),
  });
}

export function makeArrowBuffer(table: Table): ArrayBuffer {
  const ipc = tableToIPC(table);
  return ipc.buffer.slice(ipc.byteOffset, ipc.byteOffset + ipc.byteLength);
}
