#!/usr/bin/env python

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('SparkHomework.com').getOrCreate()

flights = spark.read.option("header",True).csv("gs://ik-hw-b/HW6/flights.csv")

airports = spark.read.option("header",True).csv("gs://ik-hw-b/HW6/airports.csv")

airlines = spark.read.option("header",True).csv("gs://ik-hw-b/HW6/airlines.csv")

df1 = flights.groupby('ORIGIN_AIRPORT', 'AIRLINE').agg(sum('CANCELLED')) \
             .withColumnRenamed('sum(CANCELLED)', 'cancelled')

df2 = flights.groupby('ORIGIN_AIRPORT', 'AIRLINE').count() \
             .withColumnRenamed('count', 'processed')

df3 = df1.join(df2, ['ORIGIN_AIRPORT', 'AIRLINE'], how='inner')

df4 = df3.withColumn('%_cancelled', round(df3.cancelled/df3.processed*100, 2))

a = df4.join(airports, df4.ORIGIN_AIRPORT == airports.IATA_CODE, how='inner') \
       .select(df4['*'], airports['AIRPORT'])

df5 = a.join(airlines, a.AIRLINE == airlines.IATA_CODE, how='inner') \
       .select(airports['AIRPORT'], airlines['AIRLINE'], 'cancelled', 'processed', '%_cancelled') \
       .sort(a['%_cancelled'].desc())

# percentage of canceled flights per origin airport per airline
df5.write.format('json').save('gs://ik-hw-b/HW6/all_airports.json')

# Waco Regional Airport
df6 = df5.filter(df5.AIRPORT == 'Waco Regional Airport')

df6.write.csv('gs://ik-hw-b/HW6/waco_airport.csv')


# total number of flights per airline for debugging
df7 = df2.join(airlines, df2.AIRLINE == airlines.IATA_CODE, how='inner') \
         .select(airlines['AIRLINE'], 'processed')

df_debug = df7.groupby('AIRLINE').sum('processed') \
              .withColumnRenamed('sum(processed)', 'total number of flights')

df_debug.write.csv('gs://ik-hw-b/HW6/task2_debug.csv')
