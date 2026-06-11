FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Install envsubst
RUN apt-get update -qq \
 && apt-get install -y -qq gettext-base \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Download Iceberg + AWS JARs directly into Spark jars dir
RUN curl -sSL -o $SPARK_HOME/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
 && curl -sSL -o $SPARK_HOME/jars/iceberg-aws-bundle-1.7.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar \
 && curl -sSL -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -sSL -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN chown -R jovyan:users $SPARK_HOME

USER jovyan