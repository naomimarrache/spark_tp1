package com.fakir.samples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.io.StdIn.readLine
//import org.apache.spark.sql._//pour tout avoir


object dataframeExercice1 {



  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    import org.apache.spark.sql.functions._
    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    //Question 1 : Lire le fichier en inférant les types
    val df:DataFrame = sparkSession.read.option("delimiter", ",").option("inferSchema", true).option("header", true).csv("data.csv")
    //Question 2 : Combien avons-nous de produits ayant un prix unitaire supérieur à 500 et plus de 3000 unités vendues ?
    val products = df.filter(col("Unit Price") >= 500).filter(col("Units Sold") >= 3000).count()
    println(products)
    //Question 3 : Faites la somme de tous les produits vendus valant plus de $500 en prix unitaire
    val sum = df.filter(col("Unit Price") >= 100).groupBy("Unit Price").sum("Units Sold")
    sum.show
    //Question 4 : Quel est le prix moyen de tous les produits vendus ? (En groupant par item_type)
    val mean = df.groupBy("Item Type").mean("Unit Price")
    mean.show
    //Question 5 : Créer une nouvelle colonne, "total_revenue" contenant le revenu total des ventes par produit vendu.
    val revenue = df.withColumn("total_revenue", col("Units Sold") * col("Unit Price"))
    //revenue.show
    //Question 6 : Créer une nouvelle colonne, "total_cost", contenant le coût total des ventes par produit vendu.
    val cost = revenue.withColumn("total_cost", col("Units Sold") * col("Unit Cost"))
    //cost.show
    //Question 7 : Créer une nouvelle colonne, "total_profit", contenant le bénéfice réalisé par produit.
    val profit = cost.withColumn("Total Profit", col("total_revenue") - col("total_cost"))
    //unit price discount
    val discount = profit.withColumn("unit_price_discount", when(col("Units Sold") > 3000, col("Unit Price") * 0.7).otherwise(col("Unit Price") * 0.9))
    //Question8 :  Créer une nouvelle colonne, "unit_price_discount", qui aura comme valeur "unit_price" si le nombre d'unités vendues est plus de 3000.
    val q8 = discount.withColumn("total_revenue", col("Units Sold") * col("unit_price_discount"))
    //Question 9 : Créer une nouvelle colonne, "unit_price_discount", qui aura comme valeur "unit_price" si le nombre d'unités vendues est plus de 3000.
    val q9 = q8.withColumn("total_profit", when(col("Units Sold") > 3000, col("Units Sold") * (col("Unit Price") - col("Unit Cost"))))

    //récuperer nomn de colonne
    df.columns.foreach(println)
    // pour créernun parquet il ne faut pas que ls noms de colonne continenent des espaces

    val columsWithoutSpaces = df.columns.map(elem => elem.replaceAll(" ","_"))
    columsWithoutSpaces.foreach(println)

    // avec _* => Array("a","b","c") ==> "a" , "b", "c"
    //toDF pour renommer les noms des colonnes
    val dfWithRightColumnNames = df.toDF(columsWithoutSpaces:_*)
    dfWithRightColumnNames.show()
    dfWithRightColumnNames.write.partitionBy("Sales_Channel").mode(SaveMode.Overwrite).parquet("result")
    val parquetDF = sparkSession.read.parquet("result")
    parquetDF.printSchema()
    parquetDF.show()


  }
}