package mx.santandertec.data.commons

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, concat, sha2}
//import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog

class Extractor {

  def readCSV(spark: SparkSession, inputPath: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .load(inputPath)
  }

  def movingAverage(day: Int, dataframe: DataFrame): DataFrame = {
    val dfHash = dataframe.withColumn("hash_column", sha2(concat(dataframe.columns.map(col): _*), 512))
    val dfAvg = dfHash.withColumn("movingAverage", avg(dataframe("open"))
      .over(Window.partitionBy("ticker").orderBy("date").rowsBetween(-day, day)))
      .persist()
    val df = dfAvg.select("ticker", "open", "movingAverage", "date", "hash_column")
    df.show()
    df
  }

  def writeDF(dataframe: DataFrame, outPath: String): Unit = {
    dataframe.write.partitionBy("ticker")
      .mode("overwrite")
      .parquet(outPath)
  }

  def catalog: String =
    s"""{
       |"table":{"namespace":"default", "name":"wallstreet"},
       |"rowkey":"ticker",
       |"columns":{
       |"ticker":{"cf":"rowkey", "col":"ticker", "type":"string"},
       |"open":{"cf":"ticker", "col":"open", "type":"string"},
       |"movingAverage":{"cf":"ticker", "col":"movingAverage", "type":"string"},
       |"date":{"cf":"ticker", "col":"date", "type":"string"},
       |"hash_column":{"cf":"ticker", "col":"hash_column", "type":"string"}
       |}
       |}""".stripMargin

  /*
  def writeHbase(dataFrame: DataFrame): Unit = {
    dataFrame.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "4"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
 */
  def writeHbase(dataFrame: DataFrame) = {
    dataFrame.write.format("org.apache.hadoop.hbase.spark")
      .option("hbase.columns.mapping",
        "ticker STRING :key, open STRING ticker:open, " +
          "movingAverage STRING ticker:movingAverage, date STRING ticker:date, " +
      "hash_column STRING ticker:hash_column")
      .option("hbase.table", "wallstreet")
      .option("hbase.spark.use.hbasecontext", false)
      .save()
  }

}
