package com.spark.rdd
import org.apache.spark.{SparkConf, SparkContext}

// written this program using inteliji IDE
//@Author Amol Jadhav

object SortById {

  def main(args:Array[String]): Unit =
  {


    // create conf object
    val conf=new SparkConf();
    conf.setMaster("local")

    //set app name
    conf.setAppName("SortById");
    val sc=new SparkContext(conf);
    val orderItems= sc.textFile("C://-localpath-//employee.csv");
    val orderItemArray=orderItems.map(x=>x.split(",")).collect();

    // sort the employee by the id
    val sortedArray=orderItemArray.sortBy(x=>x(0));

    // filter the  sorted employee by state CA
    val filterRdd=sortedArray.filter(x=>x(6).contains("CA"));

    // print the employee details
    filterRdd.take(200).foreach(tup=>println(tup(0),tup(1),tup(2),tup(3),tup(4),tup(5),tup(6),tup(7)))

  }
}
