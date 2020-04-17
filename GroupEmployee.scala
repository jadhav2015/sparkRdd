package com.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupEmployee {

  def main(args:Array[String]): Unit =
  {
    // create conf object
    val conf=new SparkConf();
    conf.setMaster("local")

    //set app name
    conf.setAppName("SortById");
    val sc=new SparkContext(conf);
    val employeeRdd= sc.textFile("C://BigData//employee.csv");

    //remove header in rdd
    def removeHeader(str:String):Boolean={
    return !str.contains("first_name")
  }

    // remove header
    val filterRdd=employeeRdd.filter(removeHeader)


   //creating array of the employee
    val employeeRddArray:RDD[Array[String]]=filterRdd.map(x=>x.split(","))

    // creating  pair rdd and applying group by state

    val employeeRddString=employeeRddArray.map(x=>(x(6)+" ",(x(0)+x(1)+x(2)+x(3)+x(4)+x(5)))).groupByKey()

    // printing the content
    employeeRddString.foreach(x=>println(x));

  }
}
