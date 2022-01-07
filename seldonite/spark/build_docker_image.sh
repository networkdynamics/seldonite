#!/bin/bash
BIGDL_REPO="$1"
docker build --build-arg SPARK_VERSION=3.1.2 --build-arg HADOOP_VERSION=3.2 --build-arg PYTHON_ENV_NAME=tf2 --build-arg JDK_URL=https://github.com/AdoptOpenJDK/openjdk8-upstream-binaries/releases/download/jdk8u242-b08/OpenJDK8U-jdk_x64_linux_8u242b08.tar.gz --build-arg JDK_VERSION=8u242b08 -t bigdl-k8s-spark-3.1.2-hadoop-3.2.0 "$BIGDL_REPO"/docker/bigdl-k8s/