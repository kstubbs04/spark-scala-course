package com.strategylabs.spark
      
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

class PurchaseByCustomer {
  
  def main(arguments: Array[String]){
    
    val logger = Logger.getLogger(this.getClass())
    logger.setLevel(Level.ERROR)
    
    val spark = new SparkContext("local[*]", "PurchaseByCustomer")
    
    val data = spark.textFile("../spark-scala/customer-orders.csv")
    
    
    
  }
}