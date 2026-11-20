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

// ── Databricks JDBC catalog client ───────────────────────────────────────────
// Client for Databricks Unity Catalog via the Simba/Databricks JDBC driver
// (jdbc:databricks://). PAT authentication is passed as user="token" with the
// PAT as the password, matching the Databricks JDBC driver convention.
// Type mapping covers Databricks / Spark SQL native types.

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

public class JdbcDatabricksClient extends JdbcClient {

    protected JdbcDatabricksClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    public String getTestQuery() {
        return "SELECT 1";
    }

    @Override
    protected String[] getTableTypes() {
        return new String[] {"TABLE", "VIEW", "EXTERNAL TABLE"};
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String typeName = fieldSchema.getDataTypeName().orElse("STRING").toUpperCase();
        switch (typeName) {
            case "BYTE":
            case "TINYINT":
                return Type.TINYINT;
            case "SHORT":
            case "SMALLINT":
                return Type.SMALLINT;
            case "INT":
            case "INTEGER":
                return Type.INT;
            case "LONG":
            case "BIGINT":
                return Type.BIGINT;
            case "FLOAT":
            case "REAL":
                return Type.FLOAT;
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return Type.DOUBLE;
            case "DECIMAL":
            case "NUMERIC": {
                int prec  = fieldSchema.getColumnSize().orElse(38);
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                if (scale == 0 && prec <= 18) {
                    return Type.BIGINT;
                }
                return ScalarType.createDecimalV3Type(Math.min(prec, 38), scale);
            }
            case "BOOLEAN":
                return Type.BOOLEAN;
            case "DATE":
                return ScalarType.createDateV2Type();
            case "TIMESTAMP":
            case "TIMESTAMP_NTZ":
                return ScalarType.createDatetimeV2Type(6);
            case "CHAR": {
                int len = fieldSchema.getColumnSize().orElse(1);
                return ScalarType.createCharType(Math.min(len, ScalarType.MAX_CHAR_LENGTH));
            }
            case "VARCHAR": {
                int len = fieldSchema.getColumnSize().orElse(0);
                if (len <= 0 || len > ScalarType.MAX_VARCHAR_LENGTH) {
                    return ScalarType.createStringType();
                }
                return ScalarType.createVarcharType(len);
            }
            case "STRING":
            case "TEXT":
            case "BINARY":
            case "VARBINARY":
                return ScalarType.createStringType();
            case "ARRAY":
            case "MAP":
            case "STRUCT":
            case "VARIANT":
            case "OBJECT":
                // Semi-structured / complex types stored as JSON text
                return ScalarType.createStringType();
            default:
                return ScalarType.createStringType();
        }
    }
}
