package com.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object FlatMapExample {

  //@Author  Amol Jadhav
  // flatmap merges all collection in rdd and form single rdd
  def main(args:Array[String]): Unit = {

    // create conf object
    val conf = new SparkConf();
    conf.setMaster("local")

    //set app name
    conf.setAppName("SortById");
    val sc = new SparkContext(conf);
    val employeeRdd = sc.textFile("C://BigData//employee.csv");

    //remove header in rdd
    def removeHeader(str: String): Boolean = {
      return !str.contains("first_name")
    }

    // map
    val filterMapRdd = employeeRdd.filter(removeHeader)
    val employeeMap=filterMapRdd.map(s=>s.split(","))

    // printing the content
    employeeMap.foreach(x=>println(x(0),x(1)+x(2)+x(3)+x(4)+x(5),x(6)));


    // flatMap
    val filterFlatMapRdd = employeeRdd.filter(removeHeader)
    val employeeFlatMap=filterFlatMapRdd.flatMap(s=>s.split(","))

    // printing  flatmap  content
    employeeFlatMap.foreach(x=>println(x));

  }
  }
