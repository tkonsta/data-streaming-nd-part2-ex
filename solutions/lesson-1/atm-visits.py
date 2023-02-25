from pyspark.sql import SparkSession

# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("atm-visits").getOrCreate()

#TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

#TO-DO: read the atm-visits kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible
kafkaRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","atm-visits") \
    .option("startingOffsets","earliest") \
    .load()

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
kafkaStreamingDF = kafkaRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# TO-DO: create a temporary streaming view called "ATMVisits" based on the streaming dataframe
kafkaStreamingDF.createOrReplaceTempView("ATMVisits")

# TO-DO query the temporary view with spark.sql, with this query: "select * from ATMVisits"
atmVisitsKeyValueDF=spark.sql("select * from ATMVisits")

# TO-DO: write the dataFrame from the last select statement to kafka to the atm-visit-updates topic, on the broker localhost:9092, and configure it to retrieve the earliest messages
atmVisitsKeyValueDF \
    .selectExpr("cast(key as string) as key", \
                "cast(value as string) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "atm-visit-updates") \
    .option("checkpointLocation","/tmp/kafkacheckpoint") \
    .start() \
    .awaitTermination()