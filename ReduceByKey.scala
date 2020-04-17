package com.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKey {

  def main(args:Array[String]): Unit = {
    // create conf object
    val conf = new SparkConf();
    conf.setMaster("local")

    //set app name
    conf.setAppName("SortById");
    val sc = new SparkContext(conf);
    val employeeRdd = sc.textFile("C://BigData//emp_Id_Name.csv");

    //remove header in rdd
    def removeHeader(str: String): Boolean = {
      return !str.contains("emp_id")
    }

    // remove header
    val filterRdd = employeeRdd.filter(removeHeader)
    val employeeRddArray:RDD[Array[String]]=filterRdd.map(x=>x.split(","));
    val employeePairRdd:RDD[(String,String)]=employeeRddArray.map(x=>(x(0),x(1).trim)).reduceByKey((x,n)=>x+"jj").sortByKey(ascending = true)

    // printing the content
    employeePairRdd.foreach(x=>println(x));


  }
}
