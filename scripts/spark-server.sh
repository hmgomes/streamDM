#!/usr/bin/env bash
$SPARK_HOME/bin/spark-submit \
  --class "org.apache.spark.streamdm.streamDMJob" \
  --master "spark://Heitors-MacBook-Pro.local:7077" \
../target/scala-2.11/streamdm-structured-streaming-_2.11-0.2.jar \
  $1
