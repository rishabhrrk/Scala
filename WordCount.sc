package com.rishabhrrk.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.util.LongAccumulator
import org.apache.log4j._
import scala.collection.mutable.ArrayBuffer

object playingrdd {
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","playingrdd")
    
    val lines = sc.textFile("../book.txt")
    
    val line = lines.flatMap(x => (x.split(" ")))
    
    val tense = line.map(x => (x,1)).reduceByKey((x,y) => x + y)
    
    tense.foreach(println)
    
  }
}
