package com.iceberg

import org.apache.spark.sql.SparkSession

/**
 * SparkSession的使用
 */
object SparkSessionApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparkSessionApp")
      .master("local[2]").getOrCreate()

    val absolutePath = "file:///home/vaderwang/software/spark-2.3.0/examples/src/main/resources/people.json"

    val relativePath = "data/people.json"

    val people = spark.read.json(relativePath)

    people.show()

    spark.stop()
  }
}
