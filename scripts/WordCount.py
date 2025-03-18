import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, concat_ws

spark = SparkSession.builder.appName("WordCount").getOrCreate()
sc = spark.sparkContext

#Verify if the current input follows the format: "python WordCount.py <path to file>"
if len(sys.argv) != 2:
    print("Follow usage format \"python WordCount.py <path to file>\" ")
    sys.exit(1)

input_file = sys.argv[1]

df = spark.read.text(input_file)

df_count = (
    df.withColumn('word', explode(split(col('value'), ' ')))
    .groupBy('word')
    .count()
    .sort('count', ascending=False)
)

df_count.show()

spark.stop()
