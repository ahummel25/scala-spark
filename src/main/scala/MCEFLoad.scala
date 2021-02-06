import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import utils.Struct.getMCEFSchemaStruct

object MCEFLoad {

  /** Builds an RDD from MCEF file.
    */
  def buildDataFrame(args: Array[String]): Unit = {
    val master = "local"
    val appName = "MCEF"

    if (args.length > 0) {
      println(s"First arg passed: ${args(0)}")
    }

    val conf: SparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.ui.enabled", "true")

    val ss: SparkSession = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val list = List(1, 2, 3, 4)
    val list1 = List("A", "B", "C", "D")
    //apply operation to create a zip of list
    val list2 = list zip list1
    //print list
    println(list2)

    val sqlContext: SQLContext = ss.sqlContext
    val sc: SparkContext = ss.sparkContext
    val dfSchema: StructType = getMCEFSchemaStruct
    val fileRDD: RDD[String] =
      sc.textFile("src/main/resources/data/dummyhcef.txt", 6)

    val rowsRDD: RDD[Row] = fileRDD.map(line =>
      Row(
        line.substring(0, 2),
        line.substring(2, 17),
        line.substring(17, 20),
        line.substring(20, 21)
      )
    )

    val df: DataFrame = sqlContext.createDataFrame(rowsRDD, dfSchema)

    val tempTableName = "mcef_temp_table"

    df.createOrReplaceTempView(tempTableName)

    println(s"Selecting from $tempTableName")

    ss.sql("SELECT * FROM " + tempTableName).show()
  }

  def main(args: Array[String]): Unit = {
    buildDataFrame(args)
  }
}
