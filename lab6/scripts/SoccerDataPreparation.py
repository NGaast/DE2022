# Imports
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, concat, col, lit, array, udf, dense_rank
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, DateType, TimestampType
from pyspark.sql import Window as W

# Initialize and configure Spark
sparkConf = SparkConf()
sparkConf.setMaster("spark://spark-master:7077")
sparkConf.setAppName("FixturePipeline")
sparkConf.set("spark.driver.memory", "2g")
sparkConf.set("spark.executor.cores", "1")
sparkConf.set("spark.driver.cores", "1")
spark = SparkSession.builder \
        .config(conf=sparkConf).getOrCreate()

# Setup abstract Google storage FileSystem
bucket_id = "data_de2022_ng"
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set('temporaryGcsBucket', bucket_id)
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

# Load data into DataFrame
google_cloud_storage_path = 'gs://data_de2022_ng/'

df = spark.read.format("json") \
                         .option("inferSchema", "true") \
                         .option("multiLine", "true") \
                         .load(f'{google_cloud_storage_path}fixtures.json')

# Format DataFrame for easy feature extraction
response_df = df.withColumn('response', explode('response')).select('response')

# Fetch all features
fixture_table = response_df.select( \
                   col('response.fixture.id'), \
                   col('response.league.id').alias('league_id'), \
                   split(col('response.fixture.date'), 'T').getItem(0).cast(DateType()).alias('date'), \
                   split(col('response.fixture.date'), 'T').getItem(1).cast(TimestampType()).alias('time'), \
                   'response.fixture.referee', \
                   col('response.teams.away.id').alias('away_id'), \
                   col('response.teams.home.id').alias('home_id'), \
                   'response.league.round', \
                   'response.league.season', \
                   # 'response.fixture.venue.id', \
                   col('response.score.halftime.home').alias('halftime_home'), \
                   col('response.score.halftime.away').alias('halftime_away'), \
                   col('response.score.fulltime.home').alias('fulltime_home'), \
                   col('response.score.fulltime.away').alias('fulltime_away'), \
                   col('response.score.extratime.home').alias('extratime_home'), \
                   col('response.score.extratime.away').alias('extratime_away'), \
                   col('response.score.penalty.home').alias('penalty_home'), \
                   col('response.score.penalty.away').alias('penalty_away'))

# Build Round table
window_rounds = W.orderBy('round')
(
    fixture_table
    .withColumn('round_id', dense_rank().over(window_rounds))
)

window_rounds = W.orderBy('round')

fixture_table = fixture_table.withColumn('id', dense_rank().over(window_rounds))

rounds_table = fixture_table.select('id', 'round').distinct()

# Build Referee table
window_referee = W.orderBy('referee')
(
    fixture_table
    .withColumn('referee_id', dense_rank().over(window_referee))
)

window_referee = W.orderBy('referee')

fixture_table = fixture_table.withColumn('referee_id', dense_rank().over(window_referee))

referee_table = fixture_table.select('referee_id', 'referee').distinct()

# Build Season table
window_season = W.orderBy('season')
(
    fixture_table
    .withColumn('season_id', dense_rank().over(window_season))
)

window_season = W.orderBy('season')

fixture_table = fixture_table.withColumn('season_id', dense_rank().over(window_season))

referee_table = fixture_table.select('season_id', 'season').distinct()

# Build Fixture table
fixture_table = fixture_table.drop('referee')
fixture_table = fixture_table.drop('round')
fixture_table = fixture_table.drop('season')

# Build Teams table
teams_table = response_df.select('response.teams.away.id', 'response.teams.away.name').distinct().sort('id')

# Write Fixture table to BigQuery
project_id = 'de-2022-ng'

fixture_table.write.format('bigquery') \
    .option('table', '{0}.worldcup.fixture'.format(project_id)) \
    .mode('overwrite').save()

# Write Teams table to BigQuery
teams_table.write.format('bigquery') \
    .option('table', '{0}.worldcup.teams'.format(project_id)) \
    .mode('overwrite').save()

# Write Round table to BigQuery
rounds_table.write.format('bigquery') \
    .option('table', '{0}.worldcup.round'.format(project_id)) \
    .mode('overwrite').save()

# Write Referee table to BigQuery
referee_table.write.format('bigquery') \
    .option('table', '{0}.worldcup.referee'.format(project_id)) \
    .mode('overwrite').save()