package com.nao.samples



import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object exercic2_df {
  def main(args: Array[String]): Unit = {
    //Q1
    Logger.getLogger("org").setLevel(Level.OFF)
    import org.apache.spark.sql.functions._
    val spark = SparkSession.builder().master("local").getOrCreate()
    //Question 1 : Lire le fichier en inférant les types
    val df:DataFrame = spark.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("data/donnees.csv")
    df.show()

    //Q2
    val newNames = Seq("nom_film", "nombre_vues", "note_film", "acteur_principal")
    val dfRenamed = df.toDF(newNames: _*)
    dfRenamed.show()

    //Q3
    //2
    val df3 = dfRenamed.filter(col("acteur_principal") === "Di Caprio")
    val q3 = df3.count()
    println("Nombre de film de Di Caprio " + q3)

    //3
    val sum_note = df3.select(col("note_film")).rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)
    val mean_note = sum_note/q3
    println("Moyenne note film di caprio" + mean_note)

    //4
    val total_view = dfRenamed.select(col("nombre_vues")).rdd.map(_(0).asInstanceOf[Long]).reduce(_+_)
    val dicaprio_view = df3.select(col("nombre_vues")).rdd.map(_(0).asInstanceOf[Long]).reduce(_+_)
    println("total_view " + total_view)
    println("dicaprio_view " + dicaprio_view)
    val pourcentage = ( dicaprio_view.toDouble / total_view.toDouble ) * 100
    println("Pourcentage de vues de Di Caprio " + pourcentage + " %" )

    //5
    val mean_note_by_film = dfRenamed.groupBy("acteur_principal").mean("note_film")
    println("Moyenne des notes par acteur " )
    mean_note_by_film.show()

    //Q4
    val dfNew = dfRenamed.withColumn("pourcentage_de_vues", (col("nombre_vues") / total_view)*100  )
    dfNew.show()





  }
}
