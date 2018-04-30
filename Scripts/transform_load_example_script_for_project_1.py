from pyspark.sql import SparkSession
import argparse, sqlite3, re
from pyspark.sql.functions import count, first
import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import math
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType

def format_delay_time(delay_sec):
  delay_sec = delay_sec if delay_sec >= 0 else 0
  hrs = int(delay_sec // 3600)
  delay_min = int(delay_sec // 60)
  min = delay_min if delay_min < 60 else 0
  sec = int(delay_sec) if delay_sec < 60 else int(delay_sec % 60)
  return format(hrs,"02d") + ":" + format(min,"02d") + ":" + format(sec,"02d")

format_sec_to_time_udf = udf(format_delay_time)

################################################################################################################################################
# reading parameters
parser = argparse.ArgumentParser()
parser.add_argument("-file_path", "--file_path", help="the folder where files are stored")
parser.add_argument("-db_path", "--db_path", help="full path of database to store output")
parser.add_argument("-date", "--date", help="yyyy-mm-dd")

args = parser.parse_args()
file_path = args.file_path
db_path = args.db_path
dt = args.date
################################################################################################################################################
spark = SparkSession.builder.getOrCreate()
sbf_df = spark.read.csv(file_path+'/'+dt+'istdaten.csv', sep= ';', header=True)
###############################################################################################################################################

def to_lower(input_string):
  try: 
    output_string = input_string.lower()
  except:
    output_string = ''
  return output_string

sbf_df1 = sbf_df
to_lower_udf = udf(to_lower, StringType())
sbf_df1 = sbf_df1.withColumn('PRODUKT_ID', to_lower_udf(sbf_df1['PRODUKT_ID']))
# 'corrected' total number of stops made by different means of transportation
sbf_df1_1 = sbf_df1
sbf_df1_1 = sbf_df1_1.groupBy('PRODUKT_ID').agg(count('PRODUKT_ID').alias('total_number_of_stops'))
# number of stops per FAHRT (per transportation) 
sbf_df1_2 = sbf_df1
sbf_df1_2 = sbf_df1_2.groupBy(['FAHRT_BEZEICHNER','PRODUKT_ID']).agg(count('PRODUKT_ID').alias('number_of_stops_per_journey'))
# total number of FAHRTs (journeys) per PRODUKT_ID (type of transportation)
sbf_df1_2 = sbf_df1_2.groupBy(['PRODUKT_ID']).agg(count('PRODUKT_ID').alias('total_number_of_journeys'))
# merge two dataframes
sbf_df1_combined = sbf_df1_1.join(sbf_df1_2, sbf_df1_1['PRODUKT_ID']==sbf_df1_2['PRODUKT_ID'],how='full_outer').select(sbf_df1_1.PRODUKT_ID, sbf_df1_1.total_number_of_stops, sbf_df1_2.total_number_of_journeys).sort('total_number_of_journeys', ascending=False)
sbf_df1_combined = sbf_df1_combined.na.drop().collect()

sbf_df2 = sbf_df
sbf_df2 = sbf_df2.select(['HALTESTELLEN_NAME', 'ANKUNFTSZEIT', 'AN_PROGNOSE', 'AN_PROGNOSE_STATUS'])
sbf_df2 = sbf_df2.filter((col("AN_PROGNOSE_STATUS") != "UNBEKANNT") & (col("ANKUNFTSZEIT").isNull() == False) & (col("AN_PROGNOSE").isNull() == False))
# calculate delay in seconds for all entries. If it's 
# positive number - the vehicle arrived later than expected
# negative number - the vehicle arrived earlier than expected
time_format_ANKUNFTSZEIT = "dd.MM.yyyy HH:mm"
time_format_AN_PROGNOSE  = "dd.MM.yyyy HH:mm:ss"
timeDiff = (unix_timestamp('ANKUNFTSZEIT', format=time_format_ANKUNFTSZEIT) - unix_timestamp('AN_PROGNOSE', format=time_format_ANKUNFTSZEIT))
sbf_df2 = sbf_df2.withColumn("Delay_In_Sec", timeDiff)
sbf_df2 = sbf_df2.withColumn("Delay_In_Sec", sbf_df2['Delay_In_Sec'].cast(IntegerType()))
# sub-question 1: average delay per stop
# to calculate the average delay it was chosen to leave all the occurences when the vehicles arrived earlier than expected, and set the delay to 0 
def format_delay(delay):
  return delay if delay > 0 else 0
format_delay_udf = udf(format_delay, IntegerType())
sbf_df2_1 = sbf_df2.withColumn('Delay_In_Sec', format_delay_udf(sbf_df2['Delay_In_Sec']))
sbf_df2_1 = sbf_df2_1.groupBy('HALTESTELLEN_NAME').agg(avg('Delay_In_Sec').alias('Avg_Delay_In_Sec')).sort('Avg_Delay_In_Sec', ascending=False)
# sub-question 2: location with most delays
sbf_df2_2 = sbf_df2.filter(col("Delay_In_Sec") > 0)
sbf_df2_2 = sbf_df2_2.groupBy('HALTESTELLEN_NAME').agg(count('HALTESTELLEN_NAME').alias('Number_Of_Delays')).sort('Number_Of_Delays', ascending=False)
sbf_df2_combined = sbf_df2_1.join(sbf_df2_2, sbf_df2_1['HALTESTELLEN_NAME']==sbf_df2_2['HALTESTELLEN_NAME'],how='left_outer').select(sbf_df2_1.HALTESTELLEN_NAME, sbf_df2_1.Avg_Delay_In_Sec, sbf_df2_2.Number_Of_Delays).sort('Avg_Delay_In_Sec', ascending=False)
sbf_df2_combined = sbf_df2_combined.na.drop().collect()

sbf_df3 = sbf_df
sbf_df3 = sbf_df3.select(['FAHRT_BEZEICHNER', 'HALTESTELLEN_NAME', 'ANKUNFTSZEIT'])
sbf_df3 = sbf_df3.filter(sbf_df3.ANKUNFTSZEIT.isNotNull())
time_format_ANKUNFTSZEIT = "dd.MM.yyyy HH:mm"
sbf_df3 = sbf_df3.withColumn("ANKUNFTSZEIT_datetime", unix_timestamp('ANKUNFTSZEIT', format=time_format_ANKUNFTSZEIT))
sbf_df3_start = sbf_df3.groupBy(['FAHRT_BEZEICHNER']).agg(min(sbf_df3.ANKUNFTSZEIT_datetime).alias('journey_start'))
sbf_df3_end   = sbf_df3.groupBy(['FAHRT_BEZEICHNER']).agg(max(sbf_df3.ANKUNFTSZEIT_datetime).alias('journey_end'))
# adding information about journey_start and journey_end for every stop 
sbf_df3 = sbf_df3.join(sbf_df3_start, sbf_df3['FAHRT_BEZEICHNER']==sbf_df3_start['FAHRT_BEZEICHNER'], how='left_outer').select(sbf_df3.FAHRT_BEZEICHNER, sbf_df3.HALTESTELLEN_NAME, sbf_df3.ANKUNFTSZEIT_datetime, sbf_df3_start.journey_start)
sbf_df3 = sbf_df3.join(sbf_df3_end, sbf_df3['FAHRT_BEZEICHNER']==sbf_df3_end['FAHRT_BEZEICHNER'], how='left_outer').select(sbf_df3.FAHRT_BEZEICHNER, sbf_df3.HALTESTELLEN_NAME, sbf_df3.ANKUNFTSZEIT_datetime, sbf_df3.journey_start, sbf_df3_end.journey_end)
# remove all rows/stops that do not belong to either start or end of the journey
sbf_df3 = sbf_df3.filter((col("ANKUNFTSZEIT_datetime") == col("journey_start")) | (col("ANKUNFTSZEIT_datetime") == col("journey_end")))
# sort all stops in chronological order
sbf_df3 = sbf_df3.sort('ANKUNFTSZEIT_datetime', ascending=True)
# concatenate the start and end stops of every journey
sbf_df3 = sbf_df3.groupBy('FAHRT_BEZEICHNER').agg(concat_ws("__", collect_list(sbf_df3.HALTESTELLEN_NAME)).alias('journey'))
# count number of unique journeys
sbf_df3 = sbf_df3.groupBy('journey').agg(count('journey').alias('Number_of_journeys')).sort('Number_of_journeys', ascending=False)
split_col = split(sbf_df3['journey'], '__')
sbf_df3 = sbf_df3.withColumn('journey_start', split_col.getItem(0))
sbf_df3 = sbf_df3.withColumn('journey_end', split_col.getItem(1))
sbf_df3 = sbf_df3.na.drop().collect()
###############################################################################################################################################

# database settings 
Base = declarative_base()

class table_q1(Base):
    __tablename__             = 'table_q1'
    id                        = Column(String(250), primary_key=True)
    product_id                = Column(String(250), nullable=False)
    total_number_of_stops     = Column(Integer, nullable=False)
    total_number_of_journeys  = Column(Integer, nullable=False)
    
class table_q2(Base):
    __tablename__             = 'table_q2'
    id                        = Column(String(250), primary_key=True)
    hastestellen_name         = Column(String(250), nullable=False)
    avg_delay_in_sec          = Column(Float, nullable=False)
    number_of_delays          = Column(Integer, nullable=False)
    
class table_q3(Base):
    __tablename__             = 'table_q3'
    id                        = Column(String(250), primary_key=True)
    journey                   = Column(String(250), nullable=False)
    journey_start             = Column(String(250), nullable=False)
    journey_end               = Column(String(250), nullable=False)
    number_of_journeys        = Column(Integer, nullable=False)


engine = create_engine('sqlite:///'+db_path)
Base.metadata.create_all(engine)
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session   = DBSession()

################################################################################################################################################

# push data to DB
for row in sbf_df1_combined:
  pkey = row.PRODUKT_ID + dt
  tmp = table_q1(id=pkey, product_id=row.PRODUKT_ID, total_number_of_stops=row.total_number_of_stops, total_number_of_journeys=row.total_number_of_journeys)
  session.merge(tmp)

session.commit()

for row in sbf_df2_combined:
  pkey = row.HALTESTELLEN_NAME + dt
  tmp  = table_q2(id=pkey, hastestellen_name=row.HALTESTELLEN_NAME, avg_delay_in_sec=row.Avg_Delay_In_Sec, number_of_delays=row.Number_Of_Delays)
  session.merge(tmp)
  
for row in sbf_df3:
  pkey = row.journey + dt
  tmp = table_q3(id=pkey, journey=row.journey, number_of_journeys = row.Number_of_journeys,journey_start=row.journey_start, journey_end=row.journey_end)
  session.merge(tmp)
   
session.commit()
