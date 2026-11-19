// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// ── MongoDB JDBC catalog client ───────────────────────────────────────────────
// Minimal client on top of JdbcClient for MongoDB via mongodb-jdbc-2.0.2-all.jar.
// The MongoDB JDBC driver reports BSON type names (e.g. "string", "int32", "double").
// All semi-structured / unknown types fall back to STRING.

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

public class JdbcMongoDBClient extends JdbcClient {

    protected JdbcMongoDBClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    public String getTestQuery() {
        // MongoDB JDBC 2.x (Atlas SQL mode) does not support bare SELECT 1.
        // Return a minimal valid query; testConnection() is overridden below
        // to call getDriverVersion() which only needs the JDBC connection itself.
        return "SELECT 1";
    }

    /**
     * Override testConnection to avoid running a SQL query against MongoDB.
     * The MongoDB JDBC 2.x driver (Atlas SQL) does not support arbitrary SQL
     * against a self-hosted mongod.  Getting the driver version only requires
     * an open JDBC connection — no Atlas SQL commands are issued.
     */
    @Override
    public void testConnection() {
        // getJdbcDriverVersion() opens a connection and reads
        // DatabaseMetaData.getDriverVersion() — no SQL executed.
        getJdbcDriverVersion();
    }

    @Override
    protected String[] getTableTypes() {
        return new String[] {"TABLE", "VIEW"};
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        // MongoDB JDBC reports BSON type names in DATA_TYPE_NAME (lower-case in 2.x)
        String typeName = fieldSchema.getDataTypeName().orElse("string").toLowerCase();
        switch (typeName) {
            case "int":
            case "int32":
                return Type.INT;
            case "long":
            case "int64":
                return Type.BIGINT;
            case "double":
            case "bson_double":
                return Type.DOUBLE;
            case "decimal":
            case "decimal128":
            case "bson_decimal":
                return ScalarType.createDecimalV3Type(38, 10);
            case "bool":
            case "boolean":
            case "bson_bool":
                return Type.BOOLEAN;
            case "date":
            case "date_time":
            case "bson_date":
                return ScalarType.createDatetimeV2Type(3);
            case "timestamp":
            case "bson_timestamp":
                return ScalarType.createDatetimeV2Type(0);
            case "string":
            case "bson_string":
            case "symbol":
            case "bson_symbol":
                return ScalarType.createStringType();
            case "objectid":
            case "object_id":
            case "bson_objectid":
                // ObjectId is a 12-byte hex string — store as VARCHAR(24)
                return ScalarType.createVarcharType(24);
            case "array":
            case "bson_array":
            case "object":
            case "bson_object":
            case "document":
            case "binary":
            case "bson_bindata":
            case "javascript":
            case "bson_javascript":
            case "regex":
            case "bson_regex":
            default:
                // All semi-structured / unknown BSON types → STRING (JSON text)
                return ScalarType.createStringType();
        }
    }
}
