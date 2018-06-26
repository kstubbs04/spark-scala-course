package com.strategylabs.spark
      
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

case class RawEntry(customerId: Long, amountSpent: Float)

class PurchaseByCustomer {
  
  val CustomerIdIndex = 0
  val AmountSpentIndex = 2  
  
  def parseString(line: String) = {
    val row = line.split(",")
    RawEntry(row(CustomerIdIndex).toLong, row(AmountSpentIndex).toFloat)
  }
  
  
  def main(arguments: Array[String]){
    
    val logger = Logger.getLogger(this.getClass())
    logger.setLevel(Level.ERROR)
    
    val spark = new SparkContext("local[*]", "PurchaseByCustomer")
    
    val data = spark.textFile("../spark-scala/customer-orders.csv")
    
    val kvData = data.map(parseString)
    
    
  }
}