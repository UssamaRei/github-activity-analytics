# IL-05 architecture

## Integrated pipeline

```mermaid
flowchart LR
    S[GH Archive: selected samples or complete hours] --> P[Python producer]
    P --> K[Apache Kafka: github_events]
    K --> B[Spark Structured Streaming]
    B --> H[HDFS Bronze: original JSON lines]
    AF[Airflow: manual or hourly DAG] -. fixed HTTP jobs .-> R[Private job runner]
    R -. publish and verify committed offsets .-> K
    H --> V[Spark validation]
    V --> SI[Hudi Silver: valid unique events]
    V --> Q[HDFS quarantine: invalid records]
    SI --> A[Spark SQL and MLlib KMeans]
    A --> G[Hudi Gold: metrics and clusters]
    G --> E[Validated export]
    E --> D[PostgreSQL Gold snapshot]
    D --> SU[Superset real dashboard]
    R -. processing and publication .-> A
```

Docker Compose starts Kafka, a topic initializer, an HDFS NameNode and DataNode,
an HDFS directory initializer, a Spark dependency-cache initializer, the streaming
job, a one-shot sample producer, PostgreSQL, a private job runner, Airflow and Superset.
The streaming consumer starts at earliest offsets, so it can read the sample even
if the producer finishes first. Everything belongs to the Compose project `il05`.
Airflow executes Silver as a finite runner job. An optional standalone service
(`docker compose run --rm spark-silver`) is also available for manual inspection.

HDFS is an explicit option in the general guide. The first development machine
already has the Hadoop images, so this choice avoids downloading another storage
service. Spark uses `local[2]` inside its container for the first laptop demonstration;
there is no distributed Spark worker cluster yet.

## Processing and serving

All four parts are connected. Four DAGs cover complete manual/hourly processing,
sample ingestion and synthetic dashboard refresh. Real analytics excludes known
fictional sample IDs. The separate demo dashboard never substitutes for real Gold.
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
