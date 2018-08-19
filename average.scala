package com.rishabhrrk.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.log4j._

object average {
	def main(args: Array[String]){
	  Logger.getLogger("org").setLevel(Level.ERROR)
	  
	  val sc = new SparkContext("local[*]","average")
	  
	  val lines = sc.textFile("../numbers.txt")
	  
	  val line = lines.map(x => x.toInt)
	  
	  val results = line.aggregate((0,0))( (acc,value) => (acc._1+value,acc._2+1), (acc1,acc2)=>(acc1._1+acc2._1,acc1._2+acc2._2) )
	  
	  val avg = results._1/results._2
	  
	  println(avg)
	  
	}
}
