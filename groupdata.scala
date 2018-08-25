package com.rishabhrrk.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.log4j._

import java.io.StringWriter
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import au.com.bytecode.opencsv.CSVWriter

object groupdata{
  def parse(line: String)={
    val words = line.split(",")
    val name = words(0)
    val age = words(1)
    val year = words(3).toInt
    val medal = words(6).split(" ")
    (year,(name,age,medal(0),medal(1)))
  }
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","groupdata")
    val lines = sc.textFile("E:/Lab/Spark/Datasets Day 1 Onsite/med_tal.csv")
    val line = lines.map(parse)
    //line.groupByKey().foreach(println)
    val srt = line.sortByKey()
    //srt.foreach(println)
    srt.map(medal => List(medal._1.toString,medal._2._1,medal._2._2,medal._2._3,medal._2._4).toArray).mapPartitions{medal =>
      val stringwriter = new StringWriter();
      val csvwriter = new CSVWriter(stringwriter);
      csvwriter.writeAll(medal.toList)
      Iterator(stringwriter.toString)
    }.saveAsTextFile("../outp.csv")
    
  }
}