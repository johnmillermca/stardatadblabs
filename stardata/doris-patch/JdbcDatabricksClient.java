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
        // Use the JDBC numeric type code as the primary discriminator — it is more
        // reliable than the type name string which varies across Databricks driver versions.
        int jdbcType = fieldSchema.getDataType();
        switch (jdbcType) {
            case java.sql.Types.TINYINT:
                return Type.TINYINT;
            case java.sql.Types.SMALLINT:
                return Type.SMALLINT;
            case java.sql.Types.INTEGER:
                return Type.INT;
            case java.sql.Types.BIGINT:
                return Type.BIGINT;
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
                return Type.FLOAT;
            case java.sql.Types.DOUBLE:
                return Type.DOUBLE;
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC: {
                int prec  = fieldSchema.getColumnSize().orElse(38);
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                if (scale == 0 && prec <= 18) {
                    return Type.BIGINT;
                }
                return ScalarType.createDecimalV3Type(Math.min(prec, 38), scale);
            }
            case java.sql.Types.BOOLEAN:
            case java.sql.Types.BIT:
                return Type.BOOLEAN;
            case java.sql.Types.DATE:
                // Databricks JDBC driver returns DATE values as String via getObject() —
                // map to String so the BE does not allocate a date array.
                return ScalarType.createStringType();
            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
                // Databricks JDBC driver returns TIMESTAMP values as String —
                // map to String to avoid ArrayStoreException in BE getBlockAddress.
                return ScalarType.createStringType();
            case java.sql.Types.CHAR:
            case java.sql.Types.NCHAR: {
                int len = fieldSchema.getColumnSize().orElse(1);
                return ScalarType.createCharType(Math.min(len, ScalarType.MAX_CHAR_LENGTH));
            }
            case java.sql.Types.VARCHAR:
            case java.sql.Types.NVARCHAR: {
                int len = fieldSchema.getColumnSize().orElse(0);
                if (len <= 0 || len > ScalarType.MAX_VARCHAR_LENGTH) {
                    return ScalarType.createStringType();
                }
                return ScalarType.createVarcharType(len);
            }
            default:
                // All other types (STRING, TEXT, BINARY, ARRAY, MAP, STRUCT, etc.)
                // fall through to STRING — safe for all Spark SQL / Databricks types.
                return ScalarType.createStringType();
        }
    }
}
