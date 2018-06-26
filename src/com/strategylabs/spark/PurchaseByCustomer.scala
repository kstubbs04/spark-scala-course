package com.strategylabs.spark
      
import org.apache.log4j._
import org.apache.spark.SparkContext

object PurchaseByCustomer {
    
  val CustomerIdIndex = 0
  val AmountSpentIndex = 2  
  val DESC = false
  
  def parseString(line: String) = {
    val row = line.split(",")
    (row(CustomerIdIndex).toLong, row(AmountSpentIndex).toFloat)
  }
  
  
  def main(arguments: Array[String]){
    
    //Logger
    val logger = Logger.getLogger("org").setLevel(Level.ERROR)
    
    //Spark context using all cores
    val spark = new SparkContext("local[*]", "PurchaseByCustomer")
    
    //Read in raw data
    val data = spark.textFile("../spark-scala/customer-orders.csv")
    
    //Convert to a key value RDD
    val kvData = data.map(parseString)
    
    //kvData is in the format (customerId, amountSpent)
    //Group by unique key (customerId) and sum the values (amountSpent)
    val customerSpend = kvData.reduceByKey((x,y) => x + y)
    
    
    val results = customerSpend
    //Swap: (customerId, amountSpent) => (amountSpent, customerId)
    .map(entry => (entry._2, entry._1))
    .sortByKey(DESC)
    //Swap: (amountSpent, customerId) => (customerId, amountSpent)
    .map(entry => (entry._2, entry._1))
    .collect()
    
    results.map(println)
   
  }
}