package datasource

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL
import collection.JavaConversions._
/**
 * Created by fanzhe on 2017/11/17.
 */
object ReadES2Hive {
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


    val conf = new SparkConf().setAppName(args(args.length-1))
    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
    // Use HiveContext not SQLContext
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val query :String = "?q=*:*";
//    val esTable :String = "mmconsume/payevents"

    val sparkTable :String = "tmp" + System.currentTimeMillis()

    val sql = "create table "+hiveTable +" as select * from " + sparkTable +" limit "+size


    val esDF = JavaEsSparkSQL.esDF(sqlContext,esTable,query,
      mapAsJavaMap(options).asInstanceOf[java.util.Map[java.lang.String, java.lang.String]])

    esDF.registerTempTable(sparkTable)

    sqlContext.sql(sql)

    sc.stop()


  }

}
