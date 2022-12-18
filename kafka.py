import sys
import pickle
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from elasticsearch import Elasticsearch


KAFKA_TOPIC = "log-topic"
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

    if not(os.path.isfile("init")):
    	init = 0
    	fichier = open('init',"wb") 
    	pickle.dump(init,fichier,pickle.HIGHEST_PROTOCOL)
    	fichier.close()	 
    spark = SparkSession.builder.appName(sys.argv[0])\
            .config("spark.eventLog.enabled", "true")\
            .config("spark.eventLog.dir", "file:///opt/workspace/events")\
            .getOrCreate()
    es=Elasticsearch([{"host":"172.18.0.1","port":9200,"scheme":"http"}])

    # Set log-level to WARN to avoid very verbose output
    spark.sparkContext.setLogLevel('WARN')

    # schema for parsing value string passed from Kafka
    testSchema = StructType([ \
            StructField("Class",StringType()),\
            StructField("Request_Type" ,StringType()), \
            StructField("URL" ,StringType()), \
            StructField("Host" ,StringType()), \
            StructField("Client_Ip" ,StringType()), \
            StructField( "Date",StringType()), \
            StructField("User_Agent", StringType())])
    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    df_kafka = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "172.18.0.1:9092") \
        .option("subscribe", KAFKA_TOPIC) \
        .load() \
        .selectExpr("CAST(value AS STRING)")
    """
    df_kafka.selectExpr("CAST(value AS STRING)") \
    .write \
    .format("console") \
    .save()
   """
    # parse streaming data and apply a schema
    df_kafka = parse_data_from_kafka_message(df_kafka, testSchema)
    print(df_kafka.count())
   # print(df_kafka.select("test_key").show())

    df_kafka.show()
    
    # query the spark streaming data-frame that has columns applied to it as defined in the schema
    #query = df_kafka.groupBy("test_key","test_value").count()
    
    # ES Connection
    fichier = open('init',"rb")
    init = pickle.load(fichier)
    fichier.close()
    count = df_kafka.count()    
    es.indices.create(index="logs", ignore=400)
    for i in range(init,count):

        body={
            "Class":df_kafka.collect()[i].Class,
            "Request_Type":df_kafka.collect()[i].Request_Type,
            "URL":df_kafka.collect()[i].URL,
            "Client_Ip" : df_kafka.collect()[i].Client_Ip,
            "User_Agent": df_kafka.collect()[i].User_Agent,
            "Host": df_kafka.collect()[i].Host,
            "Date": df_kafka.collect()[i].Date
        }
        print(body)
    
        resp = es.index(index="logs_test",doc_type="logfile",body=body)	
    if (count > init):
        init = count
        fichier = open('init',"wb") 
        pickle.dump(init,fichier,pickle.HIGHEST_PROTOCOL)
        fichier.close()


