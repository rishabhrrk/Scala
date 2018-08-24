package com.rishabhrrk.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.log4j._
object Pets{
  def pet(line:String)={
    val record = line.split(",")
    val petid = record(0)
    val petname = record(1)
    val owner = record(3)
    (owner,(petname))
  }
  
  def owner(line:String)={
    val record = line.split(",")
    val ownerid = record(0)
    val ownername = record(1)
    val city = record(2)
    (ownerid,(ownername,city))
  }
  
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","Pets")
    val pet_lines = sc.textFile("E:/Lab/Spark/Datasets Day 1 Onsite/Pets.csv")
    val owner_lines = sc.textFile("E:/Lab/Spark/Datasets Day 1 Onsite/Owner.csv")
    val pet_line = pet_lines.map(pet)
    //val pet_pair = pet_line.map(x=> (x._3,(x._2,x._1)))
    val owner_line = owner_lines.map(owner)
    //val owner_pair = owner_line.map(x=> (x._1,x._2))
    val result = pet_line.join(owner_line)
    val desired = result.map(x=>(x._2)).filter(x=>x._2._1!="Owner Name")
    val fin = desired.collect()
    for(f <- fin.sorted){
      val ownerid = f._1
      val joined = f._2
      println(s"$joined")
    }
  }
}