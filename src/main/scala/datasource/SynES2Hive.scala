package datasource

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
 * 同步ES数据到Hive表中
 *
 * 加载部分ES数据（根据时间戳），判断Hive表是否存在，如果存在，则插入数据，否则新建表数据
 * Created by fanzhe on 2017/11/22.
 */
object SynES2Hive {

  val options = Map(
    ("es.nodes" -> "10.16.15.8"),
    ("es.port" -> "9200"),
    ("es.read.metadata" -> "true"),
    ("es.mapping.date.rich" -> "false")
  )
  val default_query: String = "?q=*:*";

  /**
   * 判断Hive表是否存在
   * @param sqlContext
   * @param table
   * @return
   */
  def exists(sqlContext: HiveContext, db:String ,table: String): Boolean =
    sqlContext.sql("show tables in "+db+" ").filter("tableName='"+table+"'").collect.size == 1

  def main(args: Array[String]) {
    if(args.length != 5){
      println("Usage: datasource.SynES2Hive <esTable> <query> <hiveDB> <hiveTable> <appName>")
      System.exit(-1)
    }
    val esTable = args(0)
    val query = args(1)
    val hiveDB = args(2)
    val hiveTable = args(3)
    val appName = args(4)

    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    // Use HiveContext not SQLContext
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val sparkTable: String = "tmp" + System.currentTimeMillis()

    //1. check if the given hiveTable exist in Hive
//    query = select * from esTable where ???

    val sql = if(!exists(sqlContext,hiveDB,hiveTable)){// 执行更新操作
     "create table " + hiveDB+"."+hiveTable + " as  "+ query.replaceAll(esTable,sparkTable) +" limit 10"
    }else{// 执行插入操作
      "INSERT INTO TABLE " + hiveDB+"."+ hiveTable +" "+ query.replaceAll(esTable,sparkTable) +" limit 10"
    }
    println("sql:"+sql)
    val esDf = sqlContext.esDF(esTable, default_query, options)

    esDf.registerTempTable(sparkTable)

    sqlContext.sql(sql)

    sc.stop()
  }
}
