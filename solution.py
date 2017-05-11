from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from decimal import *

if __name__ == "__main__":
    #checks if spark submit has been given the right amount of arguments
    if len(sys.argv) != 2:
        print("Please pass two arguments to spark-submit", file=sys.stderr)
        exit(-1)

    #create spark session
    spark = SparkSession\
        .builder\
        .appName("Edinburgh")\
        .getOrCreate()

    #rdd contains all entries from all csv files (including 8 headers)
    #There are some days in the csv that have midnight as 24:00 and others as 00:00.
    #All midnight datetimes are normalized to "00:00"
    rdd = spark.read.text(sys.argv[1]).rdd.map(lambda l: l[0].replace("24:00", "00:00") if "24:00" in l[0] else l[0])

    a = rdd.count()
    print ("Total number of rows of all csv files: "+str(a))

    #Remove the headers from rdd
    header = rdd.first()
    rdd_no_header = rdd.filter(lambda l: l != header)
    
    a = rdd_no_header.count()
    print ("Total number of rows excluding header rows (all minute data): "+str(a))

    #Dataframe schema definition
    fields = [StructField("datetime", StringType(), True),
              StructField("temperature", DecimalType(scale = 3), True),
              StructField("humidity", DecimalType(scale = 1), True)]

    schema = StructType(fields)

    #Dataframe df is built using the schema and part of the rdd. The fields kept are: date-time, surface temperature (C),relative humidity (%)
    #The date is shortnened to mm/dd hh so we can groupBy all possible hours of a year
    df = rdd_no_header.map(lambda k: k.split(",")).map(lambda p: (p[0][5:-3], Decimal(p[5]), Decimal(p[6]))).toDF(schema)
    
    #This DF contains the number of entries for each distinct mm/dd hh, sorted, starting from the one with the least entries
    count_df = df.groupBy("datetime").count().filter("datetime NOT LIKE '02/29%'").sort("count", ascending=True)
    count_df.show()

    #This DF contains the average temperature and humidity of every possible hour in a year
    avg_df = df.groupBy("datetime").mean("temperature", "humidity")
    avg_df.show()

    #This DF contains all hours of the year whose temperature was higher than 10 C and had humidity less than 75%
    #It also ommits all hours of February 29th and the hour that gets skipped due to time change.  
    target_df = avg_df.filter("datetime NOT LIKE '02/29%' AND datetime NOT LIKE '03/25 01%' AND avg(temperature) > 10 AND avg(humidity) < 75")
    target_df.show()

    #Thus, the number of rows of target_df is the number of possible hours to run the advertising campaign
    a = target_df.count()
    print ("Maximum hours to buy:")
    print (a)
    print ("Total maximum spending:")
    print (a*500)
    print ("")

    spark.stop()