package com.rishabhrrk.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._

object In_Demand {
  
  case class Demand(site:String,item:String,CustomerNumber:String,cust_seq:String,CustomerRequestedDate:String,
      DueDate:String,ShippedDate:String,QuantityOrdered:String,Ordered_Amt_USD:String,QtyDue:String,Due_Amt_USD:String,qty_shipped:String,Shipped_Amt_USD:String)
  
  def mapper(line:String):Demand={
    val fields = line.split(",")
    val dem:Demand = Demand(fields(0),fields(1),fields(2),fields(3),fields(4),fields(5),fields(6),fields(7)
        ,fields(8),fields(9),fields(10),fields(11),fields(12))
    dem
  }
  
  def main(args: Array[String]){
    
    val spark = SparkSession.builder.appName("In_Demand").master("local[*]").config("spark.sql.warehouse.dir","file:///E:/Temp").getOrCreate()
    
    import spark.implicits._
    
    val lines = spark.sparkContext.textFile("../ActualDemand.csv")
    
    val act_Dem = lines.map(mapper).toDS().cache()
    
    //act_Dem.printSchema()
    
    act_Dem.createOrReplaceTempView("demand")
    
    val results_1 = spark.sql("select item,concat(CustomerNumber,'-',cust_seq,'-',site) customer,cast(CustomerRequestedDate as date) cust_req,cast(QuantityOrdered as float) qty from demand")
    results_1.createOrReplaceTempView("results_1")
    val results_2 = spark.sql("select item,customer,month(cust_req),sum(qty) from results_1 where cust_req>current_date() group by item,customer,month(cust_req)")
    val fin = results_2.collect()
    
    fin.foreach(println)
    
    spark.stop()
  }
  
}