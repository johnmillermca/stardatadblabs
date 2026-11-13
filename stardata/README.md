# Stardata

Stardata is a distribution of [Apache Doris](https://doris.apache.org/) `branch-4.2`,
maintained by [johnmillermca](https://github.com/johnmillermca) as the analytical query engine
for the Stardatadblabs platform.

## Base version

Apache Doris `branch-4.2` (latest HEAD)

## Custom patches

The following FE source files in `stardata/doris-patch/` are patched before building the
FE image. Copy them into the Doris source tree at the matching package path before running
`mvn package`.

| Patch file | Doris source path |
|---|---|
| `doris-patch/CreateTableInfo.java` | `fe/src/main/java/org/apache/doris/nereids/trees/plans/commands/info/CreateTableInfo.java` |
| `doris-patch/ColumnDefinition.java` | `fe/src/main/java/org/apache/doris/nereids/trees/plans/commands/info/ColumnDefinition.java` |
| `doris-patch/Column.java` | `fe/src/main/java/org/apache/doris/catalog/Column.java` |
| `doris-patch/ColumnDef.java` | `fe/src/main/java/org/apache/doris/analysis/ColumnDef.java` |

### snap_id auto-injection (`CreateTableInfo.java`)

Every `CREATE TABLE` on an internal OLAP table automatically receives a leading column:

```sql
snap_id BIGINT NOT NULL AUTO_INCREMENT
```

**Rules:**
- Injected only for internal (non-external) OLAP tables — Iceberg, Hive, Paimon, etc. are
  not affected.
- If the user's DDL already declares a column named `snap_id` (case-insensitive), no
  duplicate is added and the user's definition is preserved unchanged.
- The column is inserted at index 0, so it is always the first column in the schema.
- Implemented in `CreateTableInfo.injectSnapIdColumn()`, called at the top of `validate()`
  before any key or engine analysis.

## Build

The FE and BE images are built from the official Apache Doris `branch-4.2` source.

> **Note:** Apache Doris has not yet published a `4.2.x` release image on Docker Hub.
> `Dockerfile.fe` currently uses `apache/doris:fe-latest` as a stand-in and will be
> pinned to `fe-4.2.x` once an official release image is published.

To build and push the images to the private registry:

```bash
# FE — retag upstream as Stardata 4.2
docker build -f stardata/Dockerfile.fe \
  -t 192.168.1.50:30500/apache/stardata:fe-4.2 .
docker push 192.168.1.50:30500/apache/stardata:fe-4.2

# BE — pull upstream and retag
docker pull apache/doris:be-latest
docker tag apache/doris:be-latest 192.168.1.50:30500/apache/stardata:be-4.2
docker push 192.168.1.50:30500/apache/stardata:be-4.2
```

## Deployment

Docker images are built and pushed to the private registry at `192.168.1.50:30500`:
- `apache/stardata:fe-4.2`
- `apache/stardata:be-4.2`

See [`k8s_platform`](https://github.com/johnmillermca/stardatadblabs) for the Kubernetes manifests.
