package com.fakir.samples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession



object rdd_first_step {

  /*
  def polymorph[A,B](variable1: A): B= {
    variable1
  }*/
  def majuscule(s: String, filtre: String): String = {
    if(s.contains(filtre)) s
    else s.toUpperCase
  }




  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate()


    val rdd = sparkSession.sparkContext.textFile("data")
    val majRDD = rdd.map(elem => majuscule(elem, "Juventus"))
    val barcaRDD = majRDD.filter(elem => !elem.contains("Juventus"))
    /*barcaRDD.foreach(println)*/

    val RDD = sparkSession.sparkContext.textFile("data2")
    val RDD2 = RDD.filter(elem => !elem.startsWith("a"))
    
    val RDD3 = RDD2.map((elem:String)=>(elem.split(",")(1).toInt))
    val RDD4 = RDD3.filter(elem=>(elem>2))

    RDD4.foreach(println)
    val count = RDD4.count()

    println("nombre de personnes , avec nom qui ne commence pas par a et plus de 2 enfants")

    println(count)

    println("Correction exo :Créer un fichier CSV contenant 3 champs (au moins 20 lignes):\nNom,nombre_enfants,age\ndans un premier temps, retirer toutes les personnes dont le nom commence par A, et ayant plus de 2 enfants\ncombien vous en reste-t-il ?quelle est la moyenne d'enfants que nous avons par age ?")
    val rdd1 = sparkSession.sparkContext.textFile("data/example3.csv")
    rdd1.foreach(println)
    val suprdd = rdd1.filter(elem => !elem.contains("E") || elem.split(";")(1).toDouble > 2)
    suprdd.foreach(println)
    println(suprdd.count())
    val counts = suprdd.map(item => (item.split(";")(2).toDouble, (1.0, item.split(";")(1).toDouble)) )
    val countSums = counts.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))
    val keyMeans = countSums.mapValues(avgCount => avgCount._2 / avgCount._1)
    keyMeans.foreach(println)
    //ou par KeyValue comme Jules


  }
}