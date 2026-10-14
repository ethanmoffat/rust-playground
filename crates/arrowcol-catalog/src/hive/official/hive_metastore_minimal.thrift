/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Narrow wire-compatible Rust surface derived from Apache Hive 4.0.1:
 * standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift
 * tag: rel/release-4.0.1
 *
 * Field IDs and RPC names below are copied from that authoritative IDL. Thrift
 * permits clients to omit unused optional fields and methods, so keeping this
 * surface narrow avoids vendoring thousands of unrelated metastore APIs.
 */

namespace rs hive_metastore

struct FieldSchema {
  1: optional string name,
  2: optional string type,
  3: optional string comment
}

struct EnvironmentContext {
  1: optional map<string, string> properties
}

struct Catalog {
  1: optional string name,
  2: optional string description,
  3: optional string locationUri,
  4: optional i32 createTime
}

struct CreateCatalogRequest {
  1: required Catalog catalog
}

struct GetCatalogRequest {
  1: required string name
}

struct GetCatalogResponse {
  1: required Catalog catalog
}

struct Database {
  1: optional string name,
  2: optional string description,
  3: optional string locationUri,
  4: optional map<string, string> parameters,
  8: optional string catalogName,
  9: optional i32 createTime,
  10: optional string managedLocationUri
}

struct CreateDatabaseRequest {
  1: required string databaseName,
  2: optional string description,
  3: optional string locationUri,
  4: optional map<string, string> parameters,
  8: optional string catalogName,
  9: optional i32 createTime,
  10: optional string managedLocationUri
}

struct GetDatabaseRequest {
  1: optional string name,
  2: optional string catalogName
}

struct SerDeInfo {
  1: optional string name,
  2: optional string serializationLib,
  3: optional map<string, string> parameters
}

struct StorageDescriptor {
  1: optional list<FieldSchema> cols,
  2: optional string location,
  3: optional string inputFormat,
  4: optional string outputFormat,
  5: optional bool compressed,
  6: optional i32 numBuckets,
  7: optional SerDeInfo serdeInfo,
  8: optional list<string> bucketCols,
  10: optional map<string, string> parameters
}

struct Table {
  1: optional string tableName,
  2: optional string dbName,
  3: optional string owner,
  4: optional i32 createTime,
  5: optional i32 lastAccessTime,
  6: optional i32 retention,
  7: optional StorageDescriptor sd,
  8: optional list<FieldSchema> partitionKeys,
  9: optional map<string, string> parameters,
  10: optional string viewOriginalText,
  11: optional string viewExpandedText,
  12: optional string tableType,
  14: optional bool temporary,
  15: optional bool rewriteEnabled,
  17: optional string catName
}

struct CreateTableRequest {
  1: required Table table,
  2: optional EnvironmentContext envContext,
  9: optional list<string> processorCapabilities,
  10: optional string processorIdentifier
}

struct GetTableRequest {
  1: required string dbName,
  2: required string tblName,
  4: optional string catName,
  8: optional list<string> processorCapabilities,
  9: optional string processorIdentifier,
  10: optional string engine
}

struct GetTableResult {
  1: required Table table,
  2: optional bool isStatsCompliant
}

struct AlterTableRequest {
  1: optional string catName,
  2: required string dbName,
  3: required string tableName,
  4: required Table table,
  5: optional EnvironmentContext environmentContext,
  8: optional list<string> processorCapabilities,
  9: optional string processorIdentifier,
  10: optional string expectedParameterKey,
  11: optional string expectedParameterValue
}

struct AlterTableResponse {}

exception AlreadyExistsException {
  1: optional string message
}

exception InvalidObjectException {
  1: optional string message
}

exception MetaException {
  1: optional string message
}

exception NoSuchObjectException {
  1: optional string message
}

exception InvalidOperationException {
  1: optional string message
}

service ThriftHiveMetastore {
  void create_catalog(1: CreateCatalogRequest catalog)
    throws (1: AlreadyExistsException o1, 2: InvalidObjectException o2, 3: MetaException o3)

  GetCatalogResponse get_catalog(1: GetCatalogRequest catName)
    throws (1: NoSuchObjectException o1, 2: MetaException o2)

  void create_database_req(1: CreateDatabaseRequest createDatabaseRequest)
    throws (1: AlreadyExistsException o1, 2: InvalidObjectException o2, 3: MetaException o3)

  Database get_database_req(1: GetDatabaseRequest request)
    throws (1: NoSuchObjectException o1, 2: MetaException o2)

  void create_table_req(1: CreateTableRequest request)
    throws (1: AlreadyExistsException o1, 2: InvalidObjectException o2,
            3: MetaException o3, 4: NoSuchObjectException o4)

  GetTableResult get_table_req(1: GetTableRequest req)
    throws (1: MetaException o1, 2: NoSuchObjectException o2)

  AlterTableResponse alter_table_req(1: AlterTableRequest req)
    throws (1: InvalidOperationException o1, 2: MetaException o2)
}
