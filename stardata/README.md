# Stardata

Stardata is a distribution of [Apache Doris](https://doris.apache.org/) `branch-4.2`,
maintained by [johnmillermca](https://github.com/johnmillermca) as the analytical query engine
for the Stardatadblabs platform.

## Base version

Apache Doris `branch-4.2` (latest HEAD)

No custom patches are applied. The Stardata distribution tracks upstream Doris branch-4.2
directly and adds no modifications to the FE or BE source code.

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
