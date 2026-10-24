FROM node:20-slim

# Install kubectl
RUN apt-get update && apt-get install -y curl ca-certificates && \
    curl -LO "https://dl.k8s.io/release/$(curl -sL https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && mv kubectl /usr/local/bin/kubectl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json .
RUN npm install --production
COPY server.js .
COPY public/ ./public/

ENV PORT=3000 \
    DEBEZIUM_POSTGRES_URL=http://debezium-connect-postgres.prod.svc.cluster.local:8083 \
    DEBEZIUM_ORACLE_URL=http://debezium-connect-oracle.prod.svc.cluster.local:8083 \
    DEBEZIUM_MONGODB_URL=http://debezium-connect-mongodb.prod.svc.cluster.local:8083 \
    KAFKA_BOOTSTRAP=strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092 \
    KSQLDB_URL=http://ksqldb.prod.svc.cluster.local:8088 \
    K8S_NAMESPACE=prod

EXPOSE 3000
CMD ["node","server.js"]
