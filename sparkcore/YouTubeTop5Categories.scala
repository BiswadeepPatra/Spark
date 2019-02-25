package com.elearningpoint.sparkcore

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Compute the total amount spent per customer in some fake e-commerce data. */
object YouTubeTop5Categories {
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "YouTubeTop5Categories")   
    
    val lines = sc.textFile("../YoutubeData.csv")

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val category = lines.map(x => x.toString().split("\t")(3))
    
    println("*************" + category );
    
    val flipped = category.map( x => (x ,1) ).reduceByKey( (x,y) => x + y )
    
    val totalByCustomerSorted = flipped.sortByKey()
    
    val categoryTop5 = totalByCustomerSorted.collect()
    
    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- categoryTop5) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
  }
  
}

