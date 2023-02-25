from pyspark.sql import SparkSession

# TO-DO: create a variable with the absolute path to the text file
# /home/workspace/Test.txt
file_path = "/home/workspace/Test.txt"

# TO-DO: create a Spark session
spark = SparkSession.builder.appName("HelloSpark").getOrCreate()

# TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# ******************************
# In comparison to the first version, this counts the total number of as and bs
# ******************************

logData = spark.read.text(file_path).cache()

numAs = 0
numBs = 0

# TO-DO: Define a python function that accepts row as in an input, and increments the total number of
# times the letter 'a' has been encountered (including in this row)
# TO-DO: Define a python function that accepts row as in an input, and increments the total number of
# times the letter 'b' has been encountered (including in this row)
def countA(row):
    global numAs
    numAs += row.value.count("a")


def countB(row):
    global numAs
    numAs += row.value.count("b")


logData.foreach(countA)
logData.foreach(countB)


# TO-DO: print the count for letter 'a' and letter 'b'
print("*******")
print("*******")
print("*****Lines with a: %i, lines with b: %i" % (numAs, numBs))
print("*******")
print("*******")

# TO-DO: stop the spark application
spark.stop()