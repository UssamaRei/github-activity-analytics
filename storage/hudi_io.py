"""Reusable Spark read/write helpers for the IL-05 Silver Hudi table."""


TABLE_NAME = "github_events_silver"
HUDI_WRITE_OPTIONS = {
    "hoodie.table.name": TABLE_NAME,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": "event_id",
    "hoodie.datasource.write.precombine.field": "record_order",
    "hoodie.datasource.write.partitionpath.field": "event_date",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.index.type": "GLOBAL_BLOOM",
    "hoodie.combine.before.insert": "true",
    "hoodie.metadata.enable": "false",
}


def write_silver(dataframe, path):
    """Upsert a deduplicated batch into the date-partitioned Silver table."""
    (
        dataframe.write.format("hudi")
        # Event dates may change for a corrected event ID. A global index allows
        # Hudi to find and replace the old partition's record.
        .options(**HUDI_WRITE_OPTIONS)
        .mode("append")
        .save(path)
    )


def read_silver(spark, path):
    """Return the current snapshot of the Silver Hudi table as a DataFrame."""
    return (
        spark.read.format("hudi")
        .option("hoodie.metadata.enable", "false")
        .load(path)
    )
