FROM jupyter/all-spark-notebook:spark-3.3.0
USER root
# add bigquery, kafka, and gsc dependecies
RUN echo 'spark.jars.packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.27.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0' >> "${SPARK_HOME}/conf/spark-defaults.conf"

# Copy requirements.txt
COPY requirements.txt .
# Install required python packages
RUN pip install --no-cache-dir -r requirements.txt

# Update the guava library needed by gcs connector
RUN mv ${SPARK_HOME}/jars/guava-14.0.1.jar ${SPARK_HOME}/jars/guava-14.0.1.jar.bk
COPY *.jar ${SPARK_HOME}/jars/
