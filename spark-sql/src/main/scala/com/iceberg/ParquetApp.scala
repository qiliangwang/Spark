package com.iceberg

import org.apache.spark.sql.SparkSession

/**
 * Parquet文件操作
 */
object ParquetApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparkSessionApp")
      .master("local[2]").getOrCreate()

    val dataPath = "data/users.parquet"
    /**
     * spark.read.format("parquet").load 这是标准写法
     */
    val userDF = spark.read.format("parquet").load(dataPath)

    userDF.printSchema()
    userDF.show()

    userDF.select("name","favorite_color").show

//    userDF.select("name","favorite_color").write.format("json").save("file:///home/hadoop/tmp/jsonout")

    userDF.select("name","favorite_color").write.format("json").save("data/out")

    spark.read.load(dataPath).show

    //会报错，因为sparksql默认处理的format就是parquet
    val errorPath = "data/users.json"
    spark.read.load(errorPath).show

    spark.read.format("parquet").option("path",dataPath).load().show
    spark.stop()
  }

}
