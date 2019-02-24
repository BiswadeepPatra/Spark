package com.elearningpoint.sparkcore

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQLWithJsonData2 {

case class Artist ( id:String, last_name:String, first_name:String,year_of_birth:String )

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

	// class to infer the schema.
	import spark.implicits._
	val artistDataFrame = spark.read.json("../artists.json")
	val artistDataSet = artistDataFrame.as[Artist]

			println("Here is our inferred schema:")		
			artistDataSet.printSchema()

			println("Let's select first 20 rows : ")
			artistDataSet.show()

			println("Let's select the First Name column:")
			artistDataSet.select("first_name").show()

			/*println("Filter out anyone over 23:")
			employeeDataSet.filter(employeeDataSet("age") > 23).show()

			println("Group by age:")
			employeeDataSet.groupBy("age").count().show()*/

			spark.stop()
   }
}