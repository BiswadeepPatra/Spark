package com.elearningpoint.sparkcore

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQLWithCSVFile1 {

case class FakeFriend (id:Int, name:String, age:Int, numFriends:Int)

def mapper(line:String): FakeFriend = {
	val fields = line.split(',')  

			val fakeFriend:FakeFriend = FakeFriend(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
			return fakeFriend
}

/** Our main function where the action happens */
def main(args: Array[String]) {

	// Set the log level to only print errors
	Logger.getLogger("org").setLevel(Level.ERROR)

	// Use new SparkSession interface in Spark 2.0
	val spark = SparkSession
	.builder
	.appName("SparkSQL")
	.master("local[*]")
	.config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
	.getOrCreate()

	// Convert our csv file to a DataSet, using our Person case
	// class to infer the schema.
	import spark.implicits._
	val lines = spark.sparkContext.textFile("../fakefriends.csv")
	val fakeFriend = lines.map(mapper).toDS().cache()

	// There are lots of other ways to make a DataFrame.
	// For example, spark.read.json("json file path")
	// or sqlContext.table("Hive table name")

	println("Here is our inferred schema:")
	fakeFriend.printSchema()

	println("Let's select the name column:")
	fakeFriend.select("name").show()

	println("Filter out anyone over 21:")
	fakeFriend.filter(fakeFriend("age") < 21).show()

	println("Group by age:")
	fakeFriend.groupBy("age").count().show()

	println("Make everyone 10 years older:")
	fakeFriend.select(fakeFriend("name"), fakeFriend("age") + 10).show()

	spark.stop()
}


}