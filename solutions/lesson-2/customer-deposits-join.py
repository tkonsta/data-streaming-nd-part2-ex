from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, FloatType, BooleanType, ArrayType, DateType

# TO-DO: create deposit a kafka message schema StructType including the following JSON elements:
# {"accountNumber":"703934969","amount":415.94,"dateAndTime":"Sep 29, 2020, 10:06:23 AM"}
# Cast the amount as a FloatType  -> Again shitty instructions. Use StringType()!
depositKafkaMessageSchema = StructType (
    [
        StructField("accountNumber", StringType()),
        StructField("amount", StringType()),
        StructField("dateAndTime", StringType())
    ]
)


# TO-DO: create a customer kafka message schema StructType including the following JSON elements:
# {"customerName":"Trevor Anandh","email":"Trevor.Anandh@test.com","phone":"1015551212","birthDay":"1962-01-01","accountNumber":"45204068","location":"Togo"}
customerKafkaMessageSchema = StructType (
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
        StructField("accountNumber", StringType()),
        StructField("customerLocation", StringType())
    ]
)


# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("bank-deposits").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

#TO-DO: read the atm-visits kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible
bankDepositsRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","bank-deposits") \
    .option("startingOffsets","earliest") \
    .load()

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
bankDepositsStreamingDF = bankDepositsRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe
# TO-DO: create a temporary streaming view called "BankDeposits"
# it can later be queried with spark.sql
bankDepositsStreamingDF.withColumn("value",from_json("value",depositKafkaMessageSchema)) \
    .select(col('value.*')) \
    .createOrReplaceTempView("BankDeposits")

#TO-DO: using spark.sql, select * from BankDeposits where amount > 200.00 into a dataframe
bankDepositsSelectStarDF=spark.sql("select * from BankDeposits")

#TO-DO: read the bank-customers kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible
customersRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","bank-customers") \
    .option("startingOffsets","earliest") \
    .load()

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
customersStreamingDF = customersRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe
# TO-DO: create a temporary streaming view called "BankCustomers"
# it can later be queried with spark.sql
customersStreamingDF.withColumn("value",from_json("value",customerKafkaMessageSchema)) \
    .select(col('value.*')) \
    .createOrReplaceTempView("BankCustomers")



#TO-DO: using spark.sql, select customerName, accountNumber as customerNumber from BankCustomers into a dataframe
customerSelectStarDF=spark.sql("select customerName, accountNumber as customerNumber from BankCustomers")

#TO-DO: join the customer dataframe with the deposit dataframe
customerWithDepositDF = bankDepositsSelectStarDF.join(customerSelectStarDF, expr("""
    accountNumber = customerNumber
"""
                                                                                 ))

# TO-DO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
#. +-------------+------+--------------------+------------+--------------+
#. |accountNumber|amount|         dateAndTime|customerName|customerNumber|
#. +-------------+------+--------------------+------------+--------------+
#. |    335115395|142.17|Oct 6, 2020 1:59:...| Jacob Doshi|     335115395|
#. |    335115395| 41.52|Oct 6, 2020 2:00:...| Jacob Doshi|     335115395|
#. |    335115395| 261.8|Oct 6, 2020 2:01:...| Jacob Doshi|     335115395|
#. +-------------+------+--------------------+------------+--------------+

customerWithDepositDF.writeStream.outputMode("append").format("console").start().awaitTermination()

