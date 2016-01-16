#!/usr/bin/env bash

export SPARK_HOME="/home/stefan2/spark/spark-1.6.0/"

EXAMPLE_CLASS="stefansavev.spark.example.SparkPi"

. "${SPARK_HOME}"/bin/load-spark-env.sh

set -eu

JAR_COUNT=0

JAR_PATH=./target

for f in "${JAR_PATH}"/*.jar ; do
  if [[ ! -e "$f" ]]; then
    echo "Failed to find Spark examples assembly in ${SPARK_HOME}/lib or ${SPARK_HOME}/examples/target" 1>&2
    echo "You need to build Spark before running this program" 1>&2
    exit 1
  fi
  if [[ $f == *"-javadoc"* ]]
  then
    echo "Skipping javadoc file";
  else
    SPARK_EXAMPLES_JAR="$f"
    JAR_COUNT=$((JAR_COUNT+1))
  fi
done

if [ "$JAR_COUNT" -gt "1" ]; then
  echo "Found multiple jars in ${JAR_PATH}" 1>&2
  ls "${JAR_PATH}"/*.jar 1>&2
  echo "Please remove all but one jar." 1>&2
  exit 1
fi

export SPARK_EXAMPLES_JAR

EXAMPLE_MASTER=${MASTER:-"local[*]"}

exec "${SPARK_HOME}"/bin/spark-submit \
  --master $EXAMPLE_MASTER \
  --class $EXAMPLE_CLASS \
  "$SPARK_EXAMPLES_JAR" \
  "$@"
