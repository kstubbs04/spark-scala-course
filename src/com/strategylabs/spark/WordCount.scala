package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word appears in a book as simply as possible. */
object WordCount {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")   
    
    // Read each line of my book into an RDD
    val input = sc.textFile("../spark-scala/book.txt")
    
    
    val WordRegex = "\\W+"
    // Split into words separated by a space character
    val words = input.flatMap(x => x.split(WordRegex))
    
    val lowerWords = words.map(_.toLowerCase)
    
    //Tuple every word with the integer 1
    val wordCount = lowerWords.map(x => (x, 1))
    //group by key then sum all the 1's to get a count
    .reduceByKey((x,y) => x + y)
    
    //Flip the key and value so that it's sorted by count
    val sortedCount = wordCount.map(x => (x._2, x._1)).sortByKey()
        
    // Print the results.
    sortedCount.collect().foreach(entry => println(s" ${entry._2}: ${entry._1}"))
  }
  
}

