package com.elearningpoint.sparkcore

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQLWithJsonData1 {

case class Employee ( id:BigInt, name:String, age:BigInt )

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
	val employeeDataFrame = spark.read.json("../employee.json")
	val employeeDataSet = employeeDataFrame.as[Employee]

			println("Here is our inferred schema:")		
			employeeDataSet.printSchema()

			println("Let's select first 20 rows : ")
			employeeDataSet.show()

			println("Let's select the Name column:")
			employeeDataSet.select("name").show()

			println("Filter out anyone over 23:")
			employeeDataSet.filter(employeeDataSet("age") > 23).show()

			println("Group by age:")
			employeeDataSet.groupBy("age").count().show()

			spark.stop()
   }
}