FROM node:22-bookworm-slim AS typescript
WORKDIR /build
COPY src/dags/typescript/package*.json ./
RUN npm ci --no-audit --no-fund
COPY src/dags/typescript/tsconfig.json src/dags/typescript/reconcile.ts ./
COPY src/dags/typescript/chain.ts src/dags/typescript/javascript.js ./
RUN npm run build

FROM apache/airflow:3.4.0 AS workflows
COPY --from=typescript /usr/local/bin/node /usr/local/bin/node
COPY --from=typescript --chown=airflow:root /build/settlement.min.mjs /opt/airflow/pilot-dags/
COPY --chown=airflow:root src/dags/*.py /opt/airflow/pilot-dags/
COPY --chmod=755 deploy/local/sdk-runtime.py /usr/local/bin/pilot-sdk-runtime
USER root
RUN ln -s pilot-sdk-runtime /usr/local/bin/pilot-sdk-node \
    && ln -s pilot-sdk-runtime /usr/local/bin/pilot-sdk-java
USER airflow
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/pilot-dags \
    AIRFLOW__SDK__COORDINATORS='{"ts":{"classpath":"airflow.sdk.coordinators.node.NodeCoordinator"}}' \
    AIRFLOW__SDK__QUEUE_TO_COORDINATOR='{"typescript":"ts"}'

FROM typescript AS chain-node
RUN ./node_modules/.bin/airflow-ts-pack chain.ts --outfile chain.min.mjs

FROM golang:1.25-bookworm AS chain-go
WORKDIR /build
COPY src/dags/mixed/go/ ./
RUN CGO_ENABLED=0 go tool airflow-go-pack --output /out/go-chain . -- -trimpath

FROM maven:3.9-eclipse-temurin-21 AS chain-jvm
WORKDIR /build
COPY src/dags/mixed/jvm/ ./
RUN --mount=type=cache,target=/root/.m2 mvn --batch-mode --no-transfer-progress package \
    && mv target/airflow-chain.jar target/bundle/

FROM eclipse-temurin:21-jre-jammy AS jre

FROM workflows AS mixed
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
COPY --from=chain-node --chown=airflow:root /build/chain.min.mjs /opt/airflow/pilot-dags/handlers/
COPY --from=chain-go --chown=airflow:root /out/go-chain /opt/airflow/pilot-dags/handlers/
COPY --from=chain-jvm --chown=airflow:root /build/target/bundle/ /opt/airflow/pilot-dags/handlers/jvm/
COPY --chown=airflow:root src/dags/mixed/chain.yaml /opt/airflow/pilot-dags/
RUN printf 'handlers/*\n' > /opt/airflow/pilot-dags/.airflowignore
ENV AIRFLOW__DAG_PROCESSOR__DAG_IMPORTER_CONFIGS='[{"classpath":"airflow.sdk.importers.yaml_importer.YamlDagImporter"}]' \
    AIRFLOW__SDK__COORDINATORS='{"ts":{"classpath":"airflow.sdk.coordinators.node.NodeCoordinator"},"go":{"classpath":"airflow.sdk.coordinators.executable.ExecutableCoordinator"},"java":{"classpath":"airflow.sdk.coordinators.java.JavaCoordinator","kwargs":{"java_executable":"/opt/java/openjdk/bin/java"}}}' \
    AIRFLOW__SDK__QUEUE_TO_COORDINATOR='{"typescript":"ts","golang":"go","jvm":"java"}' \
    AIRFLOW__WORKERS__EXECUTION_API_TIMEOUT=45

FROM workflows AS default
