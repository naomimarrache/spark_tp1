package com.nao.samples


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object exercice1_RDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //Q1
    val rdd1 = sparkSession.sparkContext.textFile("data/donnees.csv")
    //rdd1.foreach(println)

    //Q2
    val rdd2 = rdd1.filter(elem => elem.contains("Di Caprio"))
    //rdd2.foreach(println)
    val q2 =rdd2.count()
    println("QUESTION 2 : Nombre de film de Di Caprio"+q2)

    //Q3
    val rdd3 = rdd2.map(elem => elem.split(";")(2).toDouble)
    println("QUESTION 3 : Note moyenne Di Caprio:"+rdd3.mean())

    //Q4
    val total_view = rdd1.map(elem => elem.split(";")(1).toDouble).sum()
    println("total view" +total_view)
    val dicaprio_view = rdd2.map(elem => elem.split(";")(1).toDouble).sum()
    println("di caprio view"+dicaprio_view)
    val q4 = (dicaprio_view/total_view)*100
    println("QUESTION 4 : pourcentage de vue de di caprio"+q4)

    //Q5
    val counts = rdd1.map(elem => (elem.split(";")(3).toString, (1.0, elem.split(";")(2).toDouble)) )
    //counts.foreach(println)
    //val grouped = counts.groupByKey()
    //grouped.foreach(println)
    val countSums = counts.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))
    //countSums.foreach(println)
    val keyMeans = countSums.mapValues(avgCount => avgCount._2 / avgCount._1)
    println("QUESTION 5 : Moyenne des notes par acteur")
    keyMeans.foreach(println)

    //Q6
    val counts2 = rdd1.map(elem => (elem.split(";")(3).toString, (1.0, elem.split(";")(1).toDouble)) )
    val countSums2 = counts2.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))
    val keyMeans2 = countSums2.mapValues(avgCount => avgCount._2 / avgCount._1)
    println("QUESTION 6 : Moyenne des vues par acteur")
    keyMeans2.foreach(println)


  }
}
