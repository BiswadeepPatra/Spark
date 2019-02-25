package com.elearningpoint.sparkcore

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQLWithCSVFile2 {

case class Cust (custId:BigInt, firstName:String, lastName:String,age:BigInt, profession:String)

case class Txn (txnNo:Int, txnDate:String, custNo:Int,amount:Float, category:String,
		product:String,city:String,state:String,spendby:String)

def mapper1(line:String): Cust = {
	val fields = line.split(',') 
			val cust:Cust = Cust(fields(0).toInt, fields(1), fields(2), fields(3).toInt,fields(4))
			return cust
}

def mapper2(line:String): Txn = {
	val fields = line.split(',')
			val txn:Txn = Txn(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toFloat,fields(4),
					fields(5),fields(6),fields(7),fields(8))
					return txn
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
	val lines1 = spark.sparkContext.textFile("../custs")
	val cust = lines1.map(mapper1).toDS().cache()

	val lines2 = spark.sparkContext.textFile("../txns")
	val txn = lines2.map(mapper2).toDS().cache()

	// There are lots of other ways to make a DataFrame.
	// For example, spark.read.json("json file path")
	// or sqlContext.table("Hive table name")

	println("Here is our inferred schema:")
	cust.printSchema()
	txn.printSchema()

	println("Let's select first 20 rows : ")
	cust.show()
	txn.show()

	println("Let's select the Count no of customers by profession:")
	cust.groupBy("profession").count().show()
	
	println("Let's select the top 10 profession:")
	cust.groupBy("profession").count().orderBy("profession").limit(10).show()

  println("Let's find out Sum total amount spent by each customer:")
	txn.groupBy("custNo").sum("amount").as("totalSpend").show();
	
  println("Let's Join the transactions with customer details : ")
  
  println("Let's Select the required fields from the join for final output: ")
  
	/* println("Group by age:")
	fakeFriend.groupBy("age").count().show()

	println("Make everyone 10 years older:")
	fakeFriend.select(fakeFriend("name"), fakeFriend("age") + 10).show()*/

	spark.stop()
}


}