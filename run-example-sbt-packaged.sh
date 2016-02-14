#!/bin/bash

set -eu

MY_JAR=target/scala-2.11/simple-project_2.11-1.6.0.jar
export SPARK_HOME="/home/stefan2/spark/spark-1.6.0/"

${SPARK_HOME}/bin/spark-submit \
  --class "stefansavev.spark.example.SparkPi" \
  --master local[4] \
  $MY_JAR


