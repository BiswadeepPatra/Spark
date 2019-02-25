package com.elearningpoint.sparkcore

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import java.util.Date;
import  java.text.SimpleDateFormat;

/** Compute the days on which each basement has more trips. */

object UberDataAnalysisWithTrips {

	val format = new java.text.SimpleDateFormat("MM/dd/yyyy");
	val days = Array("Sun","Mon","Tue","Wed","Thu","Fri","Sat");

	/** Convert input data to (basement,Day, Trips Count) tuples */
	def extractUderData(line: String) = {
		val fields = line.split(",")
				val basement = fields(0)
				val date = format.parse(fields(1));
		var basementWithDay = basement + ","+days(date.getDay()) 
				println("************************" + basementWithDay)
				val trips = fields(3)
				println("************************" + trips.toInt)		
				(basementWithDay,trips.toInt)
	}

	/** Our main function where the action happens */
	def main(args: Array[String]) {

		// Set the log level to only print errors
		Logger.getLogger("org").setLevel(Level.ERROR)

		// Create a SparkContext using every core of the local machine
		val sc = new SparkContext("local[*]", "TotalSpentByCustomer")
		val input = sc.textFile("../UderData.csv")

		val mappedInput = input.map(extractUderData)
		val totalTrips = mappedInput.reduceByKey( (x,y) => x + y )
		val totalTripsSorted = totalTrips.sortByKey()

		val results = totalTripsSorted.collect()

		// Print the results.
		results.foreach(println)
	}

}

