#!/bin/bash
BIGDL_REPO="$1"
docker build --build-arg SPARK_VERSION=3.1.2 --build-arg HADOOP_VERSION=3.2 --build-arg PYTHON_ENV_NAME=tf2 --build-arg JDK_URL=https://builds.openlogic.com/downloadJDK/openlogic-openjdk/8u292-b10/openlogic-openjdk-8u292-b10-linux-x64.tar.gz --build-arg JDK_VERSION=8u292-b10 -t bigdl-k8s-spark-3.1.2-hadoop-3.2.0 "$BIGDL_REPO"/docker/bigdl-k8s/