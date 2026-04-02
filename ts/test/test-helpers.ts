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
  new Field("partitionId", new Uint32(), false),
  new Field("label", new Utf8(), false),
  new Field("pkKey", new Utf8(), false),
  new Field("pkValue", new Utf8(), false),
  new Field("vid", new Utf8(), false),
  new Field("alive", new Bool(), false),
  new Field("latestSeq", new Uint64(), false),
  new Field("repo", new Utf8(), true),
  new Field("rkey", new Utf8(), true),
  new Field("updatedAtMs", new Uint64(), false),
  new Field("propsJson", new Utf8(), true),
]);

export const EDGE_SCHEMA = new Schema([
  new Field("partitionId", new Uint32(), false),
  new Field("edgeLabel", new Utf8(), false),
  new Field("pkKey", new Utf8(), false),
  new Field("pkValue", new Utf8(), false),
  new Field("eid", new Utf8(), false),
  new Field("srcVid", new Utf8(), false),
  new Field("dstVid", new Utf8(), false),
  new Field("alive", new Bool(), false),
  new Field("latestSeq", new Uint64(), false),
  new Field("updatedAtMs", new Uint64(), false),
  new Field("propsJson", new Utf8(), true),
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
    'partitionId': rows.map(() => 0),
    label: rows.map(r => String(r.label ?? "")),
    'pkKey': rows.map(r => String(r.pkKey ?? "rkey")),
    'pkValue': rows.map(r => String(r.pkValue ?? "")),
    vid: rows.map(r => String(r.vid ?? "")),
    alive: rows.map(r => r.alive !== false),
    'latestSeq': rows.map(r => BigInt(r.latestSeq ?? 0)),
    repo: rows.map(r => String(r.repo ?? "")),
    rkey: rows.map(r => String(r.rkey ?? "")),
    'updatedAtMs': rows.map(r => BigInt(r.updatedAtMs ?? 0)),
    'propsJson': rows.map(r => String(r.propsJson ?? "{}")),
  });
}

export function makeEdgeTable(
  rows: { 'srcVid': string; 'dstVid': string; alive?: boolean }[],
): Table {
  return buildTable(EDGE_SCHEMA, {
    'partitionId': rows.map(() => 0),
    'edgeLabel': rows.map(() => "FOLLOW"),
    'pkKey': rows.map((_, i) => `pk_${i}`),
    'pkValue': rows.map((_, i) => `pv_${i}`),
    eid: rows.map((_, i) => `e_${i}`),
    'srcVid': rows.map(r => r.srcVid),
    'dstVid': rows.map(r => r.dstVid),
    alive: rows.map(r => r.alive !== false),
    'latestSeq': rows.map(() => 1n),
    'updatedAtMs': rows.map(() => 1000n),
    'propsJson': rows.map(() => "{}"),
  });
}

export function makeArrowBuffer(table: Table): ArrayBuffer {
  const ipc = tableToIPC(table);
  return ipc.buffer.slice(ipc.byteOffset, ipc.byteOffset + ipc.byteLength);
}
