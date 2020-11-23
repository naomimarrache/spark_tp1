package com.fakir.samples
import com.fakir.samples.config.ConfigParser
import com.fakir.samples.utils.SparkReaderWriter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
object SampleProgramSparkReaderWriter {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    //val sparkSession = SparkSession.builder().master("local").getOrCreate()
    //val configCli = ConfigParser.getConfigArgs(args)
    //val df = SparkReaderWriter.readData(configCli.inputPath, configCli.inputformat)
    println("Coucou nao le 100")
    val configCli = ConfigParser.getConfigArgs(args)
    //println(configCli.partitions)
    // aller dans edit configuration et mettre les programmes arguments comme suit
    //--inputPath data/data.csv --inputFormat CSV --outputPath result/result.parquet --outputFormat parquet --partitions Region
    val df = SparkReaderWriter.readData(configCli.inputPath, configCli.inputFormat)
    //transform etc!
    df.show
    val selectedDf = df.select("Region", "Country")

    SparkReaderWriter.writeData(
      selectedDf, configCli.outputPath, configCli.outputFormat,true, Seq(configCli.partitions)
    )
  }
}
