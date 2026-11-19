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

// ── Snowflake JDBC catalog client ────────────────────────────────────────────
// Minimal client on top of JdbcClient for Snowflake proprietary (native) tables.
// Type mapping covers all Snowflake native types. Fields in JdbcFieldSchema are
// Optional<> in the 4.1 API — accessed via .orElse() throughout.

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

public class JdbcSnowflakeClient extends JdbcClient {

    protected JdbcSnowflakeClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    protected String[] getTableTypes() {
        return new String[] {"TABLE", "VIEW", "EXTERNAL TABLE"};
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String typeName = fieldSchema.getDataTypeName().orElse("VARCHAR").toUpperCase();
        switch (typeName) {
            case "NUMBER":
            case "NUMERIC":
            case "DECIMAL": {
                int prec  = fieldSchema.getColumnSize().orElse(38);
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                if (scale == 0 && prec <= 18) {
                    return Type.BIGINT;
                }
                return ScalarType.createDecimalV3Type(Math.min(prec, 38), scale);
            }
            case "INT":
            case "INTEGER":
            case "BIGINT":
            case "BYTEINT":
                return Type.BIGINT;
            case "SMALLINT":
                return Type.SMALLINT;
            case "TINYINT":
                return Type.TINYINT;
            case "FLOAT":
            case "FLOAT4":
            case "FLOAT8":
            case "DOUBLE":
            case "DOUBLE PRECISION":
            case "REAL":
                return Type.DOUBLE;
            case "BOOLEAN":
                return Type.BOOLEAN;
            case "DATE":
                return ScalarType.createDateV2Type();
            case "DATETIME":
            case "TIMESTAMP":
            case "TIMESTAMP_NTZ":
            case "TIMESTAMP_LTZ":
            case "TIMESTAMP_TZ":
                return ScalarType.createDatetimeV2Type(6);
            case "TIME":
                return ScalarType.createVarcharType(32);
            case "VARCHAR":
            case "CHAR":
            case "CHARACTER":
            case "STRING":
            case "TEXT": {
                int len = fieldSchema.getColumnSize().orElse(0);
                if (len <= 0 || len > ScalarType.MAX_VARCHAR_LENGTH) {
                    return ScalarType.createStringType();
                }
                return ScalarType.createVarcharType(len);
            }
            case "BINARY":
            case "VARBINARY":
                return ScalarType.createStringType();
            case "VARIANT":
            case "OBJECT":
            case "ARRAY":
                // Store semi-structured types as STRING (JSON text)
                return ScalarType.createStringType();
            default:
                return ScalarType.createStringType();
        }
    }
}
