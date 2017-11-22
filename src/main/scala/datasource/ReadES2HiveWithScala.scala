package datasource

import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark.sql._

/**
 * 只使用Scala的类来读取ES数据
 * Created by fanzhe on 2017/11/22.
 */
object ReadES2HiveWithScala {

  val options = Map(
    ("es.nodes" -> "10.16.15.8"),
    ("es.port" -> "9200"),
    ("es.read.metadata" -> "true"),
    ("es.mapping.date.rich" -> "false")
  )

  def main(args: Array[String]) {
    val esTable = args(0)
    val hiveTable = args(1)
    val size = args(2)


    val conf = new SparkConf().setAppName(args(args.length - 1))
    val sc = new SparkContext(conf)
    // Use HiveContext not SQLContext
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val query: String = "?q=*:*";

    val sparkTable: String = "tmp" + System.currentTimeMillis()

    val sql = "create table " + hiveTable + " as select * from " + sparkTable + " limit " + size
    val esDf = sqlContext.esDF(esTable, query, options)

    esDf.registerTempTable(sparkTable)

    sqlContext.sql(sql)

    sc.stop()
  }
}
