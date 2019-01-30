package com.iceberg.basic

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object App {

  def main(args: Array[String]): Unit = {

    val array = Array(1, 2, 3, 4, 5, 6)
//
//    for (i <- array if i % 2 == 0) {
//      print(i)
//    }

    for (i <- 1 to 3; j <- 1 to 3 if i != j) {
      println(i, j)
    }
  }
}
