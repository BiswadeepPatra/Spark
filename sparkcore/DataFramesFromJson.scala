package com.elearningpoint.sparkcore

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object DataFramesFromJson {

	/** Our main function where the action happens */
	def main(args: Array[String]) {

		// Set the log level to only print errors
		Logger.getLogger("org").setLevel(Level.ERROR)		

		// Create a SparkContext using every core of the local machine
		val sc = new SparkContext("local[*]", "DataFramesFromJson")

		val sqlcontext = new org.apache.spark.sql.SQLContext(sc)

		val employee = sqlcontext.read.json("../employee.json")

		println("Here is our inferred schema:")
		employee.printSchema()

		/*println("Let's select the name column:")
		employee.select("name").show()

		println("Filter out anyone over 23:")
		employee.filter(employee("age") > 23).show()

		println("Group by age:")
		employee.groupBy("age").count().show()  */
		
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
    val people = spark.read.json("../employee.json")
    // val people = lines.toDS().cache()
    
    // There are lots of other ways to make a DataFrame.
    // For example, spark.read.json("json file path")
    // or sqlContext.table("Hive table name")
    
    println("Here is our DataSet inferred schema:")
    people.printSchema()

	}
}