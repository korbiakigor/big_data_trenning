#!/usr/bin/env python

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('SparkHomework7.com').getOrCreate()

flights = spark.read.option("header",True).csv("gs://ik-hw-b/HW7/flights.csv")

airports = spark.read.option("header",True).csv("gs://ik-hw-b/HW7/airports.csv")

airlines = spark.read.option("header",True).csv("gs://ik-hw-b/HW7/airlines.csv")

df1 = flights.groupby('ORIGIN_AIRPORT', 'AIRLINE').agg(round(avg('DEPARTURE_DELAY'), 2) \
             .alias('AVG_AIRLINE_DEPARTURE_DELAY'))

w = Window.partitionBy('ORIGIN_AIRPORT')

df2 = df1.withColumn('MAX_AVG_AIRLINE_DEPARTURE_DELAY', max('AVG_AIRLINE_DEPARTURE_DELAY').over(w))

# max_delayer_airline_per_airport
df3 = df2.filter(col('MAX_AVG_AIRLINE_DEPARTURE_DELAY') == col('AVG_AIRLINE_DEPARTURE_DELAY')) \
         .select('ORIGIN_AIRPORT', 'AIRLINE', 'AVG_AIRLINE_DEPARTURE_DELAY')

# delays_per_airport
df4 = flights.groupby('ORIGIN_AIRPORT').agg(round(avg('DEPARTURE_DELAY'), 2) \
             .alias('AVG_DEPARTURE_DELAY'), max('DEPARTURE_DELAY').alias('MAX_DEPARTURE_DELAY'))

df5 = df4.join(df3, ['ORIGIN_AIRPORT'], how='inner') \
         .select(df3['ORIGIN_AIRPORT'], 'AVG_DEPARTURE_DELAY', 'MAX_DEPARTURE_DELAY', 'AIRLINE', \
                                        'AVG_AIRLINE_DEPARTURE_DELAY')

# final_result
result = df5.join(airports, df5.ORIGIN_AIRPORT == airports.IATA_CODE, how='inner') \
            .join(airlines, df5.AIRLINE == airlines.IATA_CODE, how='inner') \
            .select(df5['ORIGIN_AIRPORT'], airports['AIRPORT'].alias('AIRPORT_NAME'), 'AVG_DEPARTURE_DELAY', 'MAX_DEPARTURE_DELAY', \
                    df5['AIRLINE'], airlines['AIRLINE'].alias('AIRLINE_NAME'), 'AVG_AIRLINE_DEPARTURE_DELAY')

# CREATE TABLE lab2_results
result.write.format('json').save('gs://ik-hw-b/HW7/lab2_results_sql.json')
