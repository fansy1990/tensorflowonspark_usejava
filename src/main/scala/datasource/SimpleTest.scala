package datasource

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL

import scala.collection.JavaConversions._

/**
  * Created by fanzhe on 2017/11/17.
  */
object SimpleTest {

   def main(args: Array[String]) {

     val input = args(0)
     val output = args(1)

     val conf = new SparkConf().setAppName(args(args.length-1))
     val sc = new SparkContext(conf)

     sc.textFile(input).flatMap(_.split(" ")).map(x => (x,1)).reduceByKey((x,y) => x+y).saveAsTextFile(output)

     sc.stop()

   }

 }
