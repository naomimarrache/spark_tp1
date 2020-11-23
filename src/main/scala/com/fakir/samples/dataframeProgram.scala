package com.fakir.samples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.StdIn.readLine

object dataframeProgram {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    //delimiter ',' par defaut
    val df: DataFrame = sparkSession.read.option("delimiter", ",").option("inferSchema", true).option("header", true).csv("data.csv")
    df.show
    val selectedDF = df.select("Region","Country")
    import org.apache.spark.sql.functions._
    df.filter(col("Country") === "Chad")

    //fontion withColumn : pour faire une nouvelle colonne avec des colonnes
    val result = df.withColumn("Units_Sold_Plus_Unit_Price",
      col("Units Sold") + col("Unit Price"))
    import sparkSession.implicits._
    val stringValues = df.select("Country").as[String].collect()
    //stringValues.foreach(println)
    stringValues.distinct.foreach(println)
    println(stringValues(0))

    /*
    /*df.printSchema()*/
    df.show
    //val selectedDF = df.select("Region","Country")
    import org.apache.spark.sql.functions._
    df.printSchema()
    //selectedDF.filter(col("Country")==="Chad").show
    //df.filter(col("Country")==="Chad").show
    //fontion withColumn : pour faire une nouvelle colonne avec des colonnes
    val result = df.withColumn("Units_Sold_Plus_Unit_Price", col("Units Sold")+col("Unit Price"))
    result.show

    //val stringValues = df.select("Country").as[String].collect()
    /*stringValues.foreach(println)
    stringValues.distinct.foreach(println)
    val element_1 = stringValues(0)
    println(element_1)
  */
*/



  }
}