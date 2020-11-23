package com.fakir.samples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.io.StdIn.readLine

object gdpr_rdd{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    val clientsRDD = sparkSession.sparkContext.textFile("data_gdpr/clients.csv")
    val achatsRDD = sparkSession.sparkContext.textFile("data_gdpr/achats.csv")

    var iter = 0

    while(iter != 6){
      val id_input = readLine("Veuillez entrer votre identifiant")
      val user_data  = clientsRDD.filter(elem => elem.split(";")(0)==id_input)
      val nom = user_data.map(elem => elem.split(";")(1))
      val prenom = user_data.map(elem => elem.split(";")(2))
      user_data.foreach(println)
      val user_achats  = achatsRDD.filter(elem => elem.split(";")(1)==id_input)
      println("Voici vos achats:")
      user_achats.foreach(println)
      val sup_achats = readLine("supprimez vos achats de la base? (yes/no)").toString.equals("yes")
      if(sup_achats){
        val newAchatsRDD = achatsRDD.filter(elem => elem.split(";")(1)!=id_input)
        newAchatsRDD.foreach(println)
        /*newAchatsRDD.saveAsTextFile("test.csv")*/

        
      }else{
        println("Vos achats sont concervés dans la base")
      }

      iter=iter+1
    }


  }
}