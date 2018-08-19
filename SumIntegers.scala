package com.rishabhrrk.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.log4j._

object playingrdd2 {
  
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","playingrdd2")
    val lines = sc.textFile("../numbers.txt")
    val line = lines.map(x => x.toInt)
    val results = line.reduce((x,y) => x + y)
    println(results)
    
    
  }
}
