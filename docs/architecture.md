# IL-05 architecture

## Running in v0.2

```mermaid
flowchart LR
    S[Sample JSONL or bounded GH Archive download] --> P[Python producer]
    P --> K[Apache Kafka: github_events]
    K --> B[Spark Structured Streaming]
    B --> H[HDFS Bronze: original JSON lines]
    AF[Airflow: part4 profile] -. fixed HTTP jobs .-> R[Private job runner]
    R -. publish and verify committed offsets .-> K
    R --> D[PostgreSQL demo: synthetic fixture]
    D --> SU[Superset demo dashboard]
```

Docker Compose starts Kafka, a topic initializer, an HDFS NameNode and DataNode,
an HDFS directory initializer, a Spark dependency-cache initializer, the streaming
job, and a one-shot sample producer.
The streaming consumer starts at earliest offsets, so it can read the sample even
if the producer finishes first. Everything belongs to the Compose project `il05`.

HDFS is an explicit option in the general guide. The first development machine
already has the Hadoop images, so this choice avoids downloading another storage
service. Spark uses `local[2]` inside its container for the first laptop demonstration;
there is no distributed Spark worker cluster yet.

## Required final architecture

```mermaid
flowchart LR
    GH[GH Archive] --> P[Producer]
    P --> K[Kafka]
    K --> ST[Spark streaming]
    ST --> B[HDFS Bronze]
    B --> SI[Hudi Silver: cleaned events and upserts]
    SI --> SP[Spark SQL and MLlib KMeans]
    SP --> GO[Hudi Gold: metrics and clusters]
    GO --> EX[Validated Spark export]
    EX --> Q[PostgreSQL Gold serving snapshot]
    Q --> SU[Superset]
    AF[Airflow] -. download and ingestion .-> P
    AF -. processing and quality .-> SP
```

Silver, analytics and KMeans remain to be implemented by Parts 2/3. Part 4 supplies
the job adapters, exporter, SQL publisher, three DAGs and reproducible dashboards.
The real Gold dashboard remains empty until upstream jobs supply valid results.
All services use the same Compose network; Airflow does not mount a Docker socket.
PostgreSQL serves a validated copy of the four Hudi Gold tables to Superset through
a read-only account. Hudi remains the authoritative analytical storage.

See [Part 4 handoffs](part4-integration.md) for exact schemas and commands.

## Technical references

- GH Archive format and hourly naming: https://www.gharchive.org/
- GitHub event common fields: https://docs.github.com/en/rest/using-the-rest-api/github-event-types
- Kafka image: https://kafka.apache.org/39/getting-started/docker/
- Kafka single-node example: https://github.com/apache/kafka/tree/3.9.1/docker/examples/docker-compose-files
- Spark Kafka integration: https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html
- Hadoop container configuration: https://github.com/big-data-europe/docker-hadoop
- Hudi 0.15 supports Spark 3.5: https://hudi.apache.org/docs/0.15.0/quick-start-guide/
