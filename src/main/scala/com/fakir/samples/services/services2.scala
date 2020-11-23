package com.fakir.samples.services

import java.io.File
import java.io._
import java.text.SimpleDateFormat
import java.util.Date

import com.fakir.samples.services.ExampleClass
import com.fakir.samples.config.ConfigParser
import com.fakir.samples.utils.SparkReaderWriter
import com.solarmosaic.client.mail.configuration.SmtpConfiguration
import com.solarmosaic.client.mail.{Envelope, EnvelopeWrappers, Mailer}
import com.solarmosaic.client.mail.content.ContentType.MultipartTypes
import com.solarmosaic.client.mail.content.{Html, Multipart, Text}
import javax.mail.internet.InternetAddress
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, udf, when}
//import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import com.fakir.samples.utils.Encoder
object services2 {

  val EncodField = udf((elem: String) => {
    Encoder.sha1(elem)
  })


  def main(args: Array[String]): Unit = {
    //recup les arguments de configurations
    val configCli = ConfigParser.getConfigArgs(args)
    //lire fichier csv
    val df = SparkReaderWriter.readData(configCli.inputPath, configCli.inputFormat, true)
    //encoder les données de user avec id spécifié
    val Encod_user_data_Df = df.withColumn("Nom", when(col("ID")===configCli.id,EncodField(col("Nom"))).otherwise(col("Nom")))
      .withColumn("Prenom", when(col("ID")===configCli.id,EncodField(col("Prenom"))).otherwise(col("Prenom")))
    Encod_user_data_Df.show
    //ecrire fichier
    SparkReaderWriter.writeData(Encod_user_data_Df, configCli.outputPath, configCli.outputFormat, true)

    //val mail = new ExampleClass()

  }

}


