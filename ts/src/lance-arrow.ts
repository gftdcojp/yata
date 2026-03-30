import {
  Bool,
  Field,
  Schema,
  Uint32,
  Uint64,
  Utf8,
  type DataType,
} from "apache-arrow";
import type { Connection, Table } from "@lancedb/lancedb";

import {
  createEmptyYataGraphTables,
  type ArrowSchemaFactory,
  type LanceCreateTableOptionsLike,
  type YataCreatedTables,
} from "./lance.js";

export const arrowJsFactory: ArrowSchemaFactory<Schema, DataType, Field> = {
  schema(fields) {
    return new Schema(fields);
  },
  field(name, type, nullable) {
    return new Field(name, type, nullable);
  },
  uint32() {
    return new Uint32();
  },
  uint64() {
    return new Uint64();
  },
  utf8() {
    return new Utf8();
  },
  bool() {
    return new Bool();
  },
};

export function createArrowSchema(fields: Field[]): Schema {
  return new Schema(fields);
}

export async function createEmptyYataGraphTablesWithArrow(
  connection: Connection,
  options?: LanceCreateTableOptionsLike,
): Promise<YataCreatedTables<Table>> {
  return createEmptyYataGraphTables(connection, arrowJsFactory, options);
}
