#!/usr/bin/env bash
set -euo pipefail
hdfs dfsadmin -safemode wait
hdfs dfs -mkdir -p /lake/bronze /lake/silver /lake/quarantine /lake/gold /checkpoints
