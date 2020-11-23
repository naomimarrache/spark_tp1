package com.fakir.samples.services

import java.io.File
import java.io._
import com.fakir.samples.config.ConfigParser
import com.fakir.samples.utils.SparkReaderWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object services1 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val configCli = ConfigParser.getConfigArgs(args)
    //println(configCli.partitions)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //val df = SparkReaderWriter.readData("input_data/file2.csv","CSV", true)
    val df = SparkReaderWriter.readData(configCli.inputPath, configCli.inputFormat, true)
    df.show
    val dfWithoutId = df.filter(col("ID") =!= configCli.id)
    dfWithoutId.show

    SparkReaderWriter.writeData(
      dfWithoutId, configCli.outputPath, configCli.outputFormat, true)
    //dfWithoutId.write.mode(SaveMode.Overwrite).csv("test_output")

/*
    //pour avoir le fichier destination csv
    val fs = FileSystem.get(new Configuration())
    val status = fs.listStatus(new Path(configCli.outputPath))
    //status.foreach(x=> println(x.getPath))
    var path_csv_file = ""
    for(path <- status){
      val lenght_path_string = path.getPath.toString.length
      val filename = path.getPath.toString
      if(filename.charAt(lenght_path_string-1).toString.equals("v")==true
        && filename.charAt(lenght_path_string-2).toString.equals("s")==true
        && filename.charAt(lenght_path_string-3).toString.equals("c")==true ){
        println(filename)
        path_csv_file = filename
      }
    }

     */

  }
}
