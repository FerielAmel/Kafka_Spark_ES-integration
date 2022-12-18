import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from elasticsearch import Elasticsearch


KAFKA_TOPIC = "topic"
def parse_data_from_kafka_message(df, schema):
    """ take a Spark Streaming df and parse value col based on <schema>, return streaming df cols in schema """
    from pyspark.sql.functions import split


    #split attributes to nested array in one Column
    col = split(df['value'], ',') 

    # expand col to multiple top-level columns
    for idx, field in enumerate(schema): 
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])


if __name__ == "__main__":
	 
    spark = SparkSession.builder.appName(sys.argv[0])\
            .config("spark.eventLog.enabled", "true")\
            .config("spark.eventLog.dir", "file:///opt/workspace/events")\
            .getOrCreate()
    es=Elasticsearch([{"host":"x.x.x.x","port":yyyy,"scheme":"http"}])

    # Set log-level to WARN to avoid very verbose output
    spark.sparkContext.setLogLevel('WARN')

    # schema for parsing value string passed from Kafka
    testSchema = StructType([ \
            StructField("Class",StringType()),\
            StructField("Date", StringType())])
    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    df_kafka = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "x.x.x.x:yyyy") \
        .option("subscribe", KAFKA_TOPIC) \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    # parse streaming data and apply a schema
    df_kafka = parse_data_from_kafka_message(df_kafka, testSchema)
    #In order to show the results on the terminal : df_kafka.show()
    
    
    # ES Connection
    count = df_kafka.count()    
    es.indices.create(index="logs", ignore=400)
    for i in range(init,count):

        body={
            "Class":df_kafka.collect()[i].Class,
            "Date": df_kafka.collect()[i].Date
        }
        print(body)
    
        resp = es.index(index="test",doc_type="test",body=body)


