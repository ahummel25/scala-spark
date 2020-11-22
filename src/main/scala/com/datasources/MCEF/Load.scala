package com.datasources.MCEF

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import com.utils.StructUtils.getMCEFSchemaStruct
import com.utils.Utils.{LogLevel, setLogLevel}

object Load {
  /** Builds an RDD from MCEF file.
   *
   */
  def buildDataFrame(): Unit = {
    val master = "local"
    val appName = "MCEF"
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

    val conf: SparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.ui.enabled", "true")
      .set("spark.sql.warehouse.dir", warehouseLocation)

    val ss: SparkSession = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val sqlContext: SQLContext = ss.sqlContext

    val sc: SparkContext = ss.sparkContext

    setLogLevel(sc, LogLevel.WARN)

    val dfSchema: StructType = getMCEFSchemaStruct

    val fileRDD: RDD[String] = sc.textFile("dummyhcef.txt", 6)

    val rowsRDD: RDD[Row] = fileRDD.map(line =>
      Row(line.substring(0, 2),
          line.substring(2, 17),
          line.substring(17, 20),
          line.substring(20, 21)
      )
    )

    val df: DataFrame = sqlContext.createDataFrame(rowsRDD, dfSchema)

    val tempTableName = "mcef_temp_table"

    df.createOrReplaceTempView(tempTableName)

    println(s"Selecting from ${tempTableName}")
    val mcefTableDF = ss.sql("SELECT * FROM " + tempTableName)
    mcefTableDF.show()

//    df.printSchema()
//
//    df.show()
  }

  def main(args: Array[String]): Unit = {
    buildDataFrame()
  }
}
