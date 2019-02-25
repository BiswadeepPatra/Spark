package com.elearningpoint.sparkcore

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQLWithRDD {

case class Aadhar (state:String, city:String, numApproved:Int, numRejected:Int)

def mapper(line:String): Aadhar = {
	val fields = line.split('\t')  

			val aadhar:Aadhar = Aadhar(fields(0), fields(1), fields(2).toInt, fields(3).toInt)
			return aadhar
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

	val lines = spark.sparkContext.textFile("../aadhar")
	val aadhar = lines.map(mapper)

	// Infer the schema, and register the DataSet as a table.
	import spark.implicits._
	val aadharDS = aadhar.toDS

	println("Here is our inferred schema:")
	aadharDS.printSchema()

	aadharDS.createOrReplaceTempView("AadharCard")

	// SQL can be run over DataSet that have been registered as a table
	println("Let's select first 20 rows : ")
	val takeTwenty = spark.sql(" SELECT * FROM AadharCard limit 20")    
	val result = takeTwenty.collect()    
	result.foreach(println)

	println("Let's select total number of cards approved by States : ")
	val numOfAppByState = spark.sql(" select state ,sum(numApproved) from AadharCard group by state ")    
	val result1 = numOfAppByState.collect()    
	result1.foreach(println)

	println("Let's select total number of cards rejected by states : ")
	val numOfRejByState = spark.sql(" select state ,sum(numRejected) from AadharCard group by state ")    
	val result2 = numOfRejByState.collect()    
	result2.foreach(println)

	println("Let's select total number of cards approved by cities : ")
	val numOfAppByCity = spark.sql(" select city ,sum(numApproved) from AadharCard group by city ")    
	val result3 = numOfAppByCity.collect()    
	result3.foreach(println)	

	println("Let's select total number of cards rejected by cities : ")
	val numOfRejByCity = spark.sql(" select city ,sum(numRejected) from AadharCard group by city ")    
	val result4 = numOfRejByCity.collect()    
	result4.foreach(println)
	
	println("Let's select top 10 of cards approved by cities : ")
	
	println("Let's select top 10 of cards rejected by cities : ")
	
	println("Let's select total number of adhaar applicant by gender : ")	
	
	println("Let's select total number of adhaar applicant by age type : ")

	spark.stop()
}
}