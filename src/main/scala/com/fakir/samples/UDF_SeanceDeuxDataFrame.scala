package com.fakir.samples
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object UDF_SeanceDeuxDataFrame {
  val transformField = udf((date: String) => {
    val formatDate = new SimpleDateFormat("yyyy-MM-dd")
    formatDate.format(new Date(date))
  })


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    //val rdd: RDD[String] = sparkSession.sparkContext.textFile("data.csv")
    //Question 1 : Lire le fichier en inférant les types
    val df:DataFrame = sparkSession.read.option("delimiter", ",").option("inferSchema", true).option("header", true).csv("data/data.csv")

    val transformedDateDf = df.withColumn("Order Date", transformField(col("Order Date")))
    transformedDateDf.show



  }
}