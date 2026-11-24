"""Finite quality gate for the complete current Silver Hudi snapshot."""

import argparse
import os
from functools import reduce

from pyspark.sql import SparkSession, functions as F
from storage.hudi_io import read_silver
from storage.silver_contract import SILVER_COLUMN_TYPES


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--hour', help='UTC source-hour identity')
    parser.parse_args()
    spark = (SparkSession.builder.appName('IL05-Silver-Quality')
             .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        silver = read_silver(spark, os.getenv('SILVER_PATH', 'hdfs://namenode:9000/lake/silver/github_events'))
        types = {field.name: field.dataType.simpleString() for field in silver.schema.fields}
        mismatches = [name for name, kind in SILVER_COLUMN_TYPES.items() if types.get(name) != kind]
        if mismatches:
            raise ValueError('Silver schema mismatch: ' + ', '.join(mismatches))
        count = silver.count()
        if not count:
            raise ValueError('Silver is empty')
        checks = [F.col(name).isNull() | (F.length(F.trim(F.col(name))) == 0)
                  for name in ('event_id', 'event_type', 'actor_login', 'repo_name', 'record_order')]
        checks += [F.col('created_at').isNull(), F.col('event_date').isNull(),
                   F.col('event_date') != F.to_date('created_at')]
        if silver.filter(reduce(lambda a, b: a | b, checks)).limit(1).count():
            raise ValueError('Silver has missing required values or inconsistent UTC dates')
        if silver.groupBy('event_id').count().filter(F.col('count') > 1).limit(1).count():
            raise ValueError('Silver has duplicate event IDs')
        print('PASS: {} unique Silver events; schema, required fields and UTC dates valid.'.format(count), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
