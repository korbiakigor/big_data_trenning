#!/usr/bin/env python

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName('SparkHomework.com').getOrCreate()

flights = spark.read.option("header",True).csv("gs://ik-hw-b/HW6/flights.csv")

airports = spark.read.option("header",True).csv("gs://ik-hw-b/HW6/airports.csv")

df1 = flights.groupby('YEAR', 'MONTH', 'DESTINATION_AIRPORT').count() \
             .sort(flights['YEAR'].cast(IntegerType()), flights['MONTH'].cast(IntegerType()))

df2 = df1.groupby('YEAR', 'MONTH').max('count') \
         .withColumnRenamed('max(count)', 'maxCount')

df3 = df1.join(df2, ['YEAR', 'MONTH'], how='inner') \
         .filter(col('count') == col('maxCount'))

result = df3.join(airports, df3.DESTINATION_AIRPORT == airports.IATA_CODE, how="left") \
            .select(df3['*'], airports['AIRPORT']) \
            .sort(df3['YEAR'].cast(IntegerType()), df3['MONTH'].cast(IntegerType())) \
            .select('YEAR', 'MONTH', 'AIRPORT', 'count') \
            .withColumnRenamed('AIRPORT', 'DESTINATION_AIRPORT')

df_debug = df1.join(airports, df1.DESTINATION_AIRPORT == airports.IATA_CODE, how="left") \
              .select(df1['*'], airports['AIRPORT']) \
              .select('YEAR', 'MONTH', 'AIRPORT', 'count') \
              .withColumnRenamed('AIRPORT', 'DESTINATION_AIRPORT') \
              .sort(df1['YEAR'].cast(IntegerType()), df1['MONTH'].cast(IntegerType()), df1['count'].desc())


# the most popular destination airport in each month
result.write.option('delimiter','\t').csv('gs://ik-hw-b/HW6/destination_airports.csv')

# statistics per each of the airports for debugging
df_debug.write.option('delimiter','\t').csv('gs://ik-hw-b/HW6/task1_debug.csv')
