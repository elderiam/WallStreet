package mx.santandertec.data.joboperation

import mx.santandertec.data.commons.Extractor
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

object WallStreetJobOperator {

  val extractor: Extractor = new Extractor
  val LOGGER: Logger = LogManager
    .getLogger("SantanderTest")

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("SantanderTest")
    .getOrCreate()
  // spark.sparkContext.setLogLevel("WARN")


  def main(args: Array[String]): Unit = {

    val pathInput = args(0)
    val nDays = args(1).toInt
    val outputPath = args(2)

    LOGGER.info(s"Read Dataset from $pathInput")
    val df = extractor.readCSV(spark, pathInput)
    LOGGER.info(s"Applied Moving Average with $nDays days")
    val movDF = extractor.movingAverage(nDays.toInt, df)
    extractor.writeDF(movDF, outputPath)
    LOGGER.info(s"Write DataFrame in $outputPath")
    extractor.writeHbase(movDF)
    LOGGER.info(s"Saving DataFrame in Hbase")


  }
}
