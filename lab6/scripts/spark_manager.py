# Imports
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, concat, col, lit, array, udf, dense_rank
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, DateType, TimestampType
from pyspark.sql import Window as W

class SparkManager:

    def __init__(self):
        self.spark = None
        self.df = None
        self.google_cloud_storage_path = 'gs://data_de2022_ng/'
        self.bucket_id = "data_de2022_ng"

    def spark_init(self):
        sparkConf = SparkConf()
        sparkConf.setMaster("spark://spark-master:7077")
        sparkConf.setAppName("FixturePipeline")
        sparkConf.set("spark.driver.memory", "2g")
        sparkConf.set("spark.executor.cores", "1")
        sparkConf.set("spark.driver.cores", "1")
        self.spark = SparkSession.builder \
                .config(conf=sparkConf).getOrCreate()

        conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        conf.set('temporaryGcsBucket', bucket_id)
        conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    def load_fixture_data(self):
        # Load data into DataFrame
        self.df = self.spark.read.format("json") \
                                .option("inferSchema", "true") \
                                .option("multiLine", "true") \
                                .load(f'{self.google_cloud_storage_path}fixtures.json')

    def explode_json(self):
        response_df = df.withColumn('response', explode('response')).select('response')
        return reponse_df