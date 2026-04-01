import {
  EDGE_LIVE_IN_TABLE,
  EDGE_LIVE_OUT_TABLE,
  EDGE_LOG_TABLE,
  VERTEX_LIVE_TABLE,
  VERTEX_LOG_TABLE,
  edgeLiveInSchema,
  edgeLiveOutSchema,
  edgeLogSchema,
  vertexLiveSchema,
  vertexLogSchema,
  type YataField,
  type YataScalarType,
  type YataTableSchema,
} from "./schema.js";

export interface ArrowSchemaFactory<TSchema = unknown, TType = unknown, TField = unknown> {
  schema(fields: TField[]): TSchema;
  field(name: string, type: TType, nullable: boolean): TField;
  uint32(): TType;
  uint64(): TType;
  utf8(): TType;
  bool(): TType;
}

export interface LanceCreateTableOptionsLike {
  mode?: "create" | "overwrite";
  existOk?: boolean;
  [key: string]: unknown;
}

export interface LanceConnectionLike<TSchema = unknown, TTable = unknown> {
  createEmptyTable(
    name: string,
    schema: TSchema,
    options?: LanceCreateTableOptionsLike,
  ): Promise<TTable>;
  openTable(name: string): Promise<TTable>;
}

export type YataGraphTableName =
  | typeof VERTEX_LOG_TABLE
  | typeof EDGE_LOG_TABLE
  | typeof VERTEX_LIVE_TABLE
  | typeof EDGE_LIVE_OUT_TABLE
  | typeof EDGE_LIVE_IN_TABLE;

export interface YataCreatedTables<TTable = unknown> {
  'yata_vertex_log': TTable;
  'yata_edge_log': TTable;
  'yata_vertex_live': TTable;
  'yata_edge_live_out': TTable;
  'yata_edge_live_in': TTable;
}

export interface YataOpenedTables<TTable = unknown> {
  'vertex_log': TTable;
  'edge_log': TTable;
  'vertex_live': TTable;
  'edge_live_out': TTable;
  'edge_live_in': TTable;
}

function mapScalarType<TType>(kind: YataScalarType, factory: ArrowSchemaFactory<unknown, TType, unknown>): TType {
  switch (kind) {
    case "uint32":
      return factory.uint32();
    case "uint64":
      return factory.uint64();
    case "utf8":
      return factory.utf8();
    case "bool":
      return factory.bool();
  }
}

export function toArrowField<TType, TField>(
  spec: YataField,
  factory: ArrowSchemaFactory<unknown, TType, TField>,
): TField {
  return factory.field(spec.name, mapScalarType(spec.type, factory), spec.nullable);
}

export function toArrowSchema<TSchema, TType, TField>(
  table: YataTableSchema,
  factory: ArrowSchemaFactory<TSchema, TType, TField>,
): TSchema {
  return factory.schema(table.fields.map((f) => toArrowField(f, factory)));
}

export function yataGraphArrowSchemas<TSchema, TType, TField>(
  factory: ArrowSchemaFactory<TSchema, TType, TField>,
): Record<YataGraphTableName, TSchema> {
  return {
    [VERTEX_LOG_TABLE]: toArrowSchema(vertexLogSchema, factory),
    [EDGE_LOG_TABLE]: toArrowSchema(edgeLogSchema, factory),
    [VERTEX_LIVE_TABLE]: toArrowSchema(vertexLiveSchema, factory),
    [EDGE_LIVE_OUT_TABLE]: toArrowSchema(edgeLiveOutSchema, factory),
    [EDGE_LIVE_IN_TABLE]: toArrowSchema(edgeLiveInSchema, factory),
  };
}

export async function createEmptyYataGraphTables<TSchema, TTable>(
  connection: LanceConnectionLike<TSchema, TTable>,
  factory: ArrowSchemaFactory<TSchema, unknown, unknown>,
  options: LanceCreateTableOptionsLike = { mode: "create", existOk: true },
): Promise<YataCreatedTables<TTable>> {
  const schemas = yataGraphArrowSchemas(factory);

  const [
    yata_vertex_log,
    yata_edge_log,
    yata_vertex_live,
    yata_edge_live_out,
    yata_edge_live_in,
  ] = await Promise.all([
    connection.createEmptyTable(VERTEX_LOG_TABLE, schemas[VERTEX_LOG_TABLE], options),
    connection.createEmptyTable(EDGE_LOG_TABLE, schemas[EDGE_LOG_TABLE], options),
    connection.createEmptyTable(VERTEX_LIVE_TABLE, schemas[VERTEX_LIVE_TABLE], options),
    connection.createEmptyTable(EDGE_LIVE_OUT_TABLE, schemas[EDGE_LIVE_OUT_TABLE], options),
    connection.createEmptyTable(EDGE_LIVE_IN_TABLE, schemas[EDGE_LIVE_IN_TABLE], options),
  ]);

  return {
    yata_vertex_log,
    yata_edge_log,
    yata_vertex_live,
    yata_edge_live_out,
    yata_edge_live_in,
  };
}

export async function openYataGraphTables<TTable>(
  connection: LanceConnectionLike<unknown, TTable>,
): Promise<YataOpenedTables<TTable>> {
  const [vertex_log, edge_log, vertex_live, edge_live_out, edge_live_in] = await Promise.all([
    connection.openTable(VERTEX_LOG_TABLE),
    connection.openTable(EDGE_LOG_TABLE),
    connection.openTable(VERTEX_LIVE_TABLE),
    connection.openTable(EDGE_LIVE_OUT_TABLE),
    connection.openTable(EDGE_LIVE_IN_TABLE),
  ]);

  return {
    vertex_log,
    edge_log,
    vertex_live,
    edge_live_out,
    edge_live_in,
  };
}

export async function openYataGraphTablesFromManifest<TTable>(
  connection: LanceConnectionLike<unknown, TTable>,
  manifest: import("./manifest.js").GraphManifest,
): Promise<YataOpenedTables<TTable>> {
  const [vertex_log, edge_log, vertex_live, edge_live_out, edge_live_in] = await Promise.all([
    connection.openTable(manifest.tables.vertex_log.table_name),
    connection.openTable(manifest.tables.edge_log.table_name),
    connection.openTable(manifest.tables.vertex_live.table_name),
    connection.openTable(manifest.tables.edge_live_out.table_name),
    connection.openTable(manifest.tables.edge_live_in.table_name),
  ]);

  return {
    vertex_log,
    edge_log,
    vertex_live,
    edge_live_out,
    edge_live_in,
  };
}
