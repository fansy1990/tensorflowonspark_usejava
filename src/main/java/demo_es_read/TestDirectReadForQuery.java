package demo_es_read;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import java.util.Arrays;
import java.util.HashMap;

/**
 * 直接用Spark Standalone 读取 ES数据\\
 * 测试增量同步sql
 * Created by fanzhe on 2017/11/27.
 */
public class TestDirectReadForQuery {
    public static void main(String[] args){
        String esTable = "twitter/doc"; //
        String query = "select * from "+esTable+" limit 10";

        HashMap<String, String> options = new HashMap<String, String>();
        options.put("es.nodes", "192.168.0.78"); // ES环境IP之一
        options.put("es.port", "9200"); // ES环境端口
        options.put("es.read.metadata", "true");
        options.put("es.mapping.date.rich", "false"); //必须，否则日期字段转换报错

        SparkConf conf = new SparkConf();
        conf.set("spark.master", "spark://server2.tipdm.com:7077");
        conf.set("spark.app.name", "read es table ");
        conf.set("spark.executor.memory","1g"); // 建议不小于z"1g"
        conf.set("spark.executor.cores","2");
        conf.set("spark.cores.max","2");
        conf.set("spark.yarn.appMasterEnv.SPARK_DIST_CLASSPATH","/etc/hadoop/conf:/etc/hadoop/conf:/etc/hadoop/conf:/opt/cloudera/parcels/CDH-5.7.3-1.cdh5.7.3.p0.5/lib/hadoop/libexec/../../hadoop/lib/*:/opt/cloudera/parcels/CDH-5.7.3-1.cdh5.7.3.p0.5/lib/hadoop/libexec/../../hadoop/.//*:/opt/cloudera/parcels/CDH-5.7.3-1.cdh5.7.3.p0.5/lib/hadoop/libexec/../../hadoop-hdfs/./:/opt/cloudera/parcels/CDH-5.7.3-1.cdh5.7.3.p0.5/lib/hadoop/libexec/../../hadoop-hdfs/lib/*:/opt/cloudera/parcels/CDH-5.7.3-1.cdh5.7.3.p0.5/lib/hadoop/libexec/../../hadoop-hdfs/.//*:/opt/cloudera/parcels/CDH-5.7.3-1.cdh5.7.3.p0.5/lib/hadoop/libexec/../../hadoop-yarn/lib/*:/opt/cloudera/parcels/CDH-5.7.3-1.cdh5.7.3.p0.5/lib/hadoop/libexec/../../hadoop-yarn/.//*:/opt/cloudera/parcels/CDH-5.7.3-1.cdh5.7.3.p0.5/bin/../lib/hadoop-mapreduce/lib/*:/opt/cloudera/parcels/CDH-5.7.3-1.cdh5.7.3.p0.5/bin/../lib/hadoop-mapreduce/.//*:/opt/cloudera/parcels/CDH-5.7.3-1.cdh5.7.3.p0.5/lib/hadoop/libexec/../../hadoop-yarn/.//*:/opt/cloudera/parcels/CDH-5.7.3-1.cdh5.7.3.p0.5/lib/hadoop/libexec/../../hadoop-yarn/lib/*:/opt/es_jars/*");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        String q = "?q=*:*"; // 支持queryString语法的过滤（若不使用过滤器则无需修改）
      //   ES中的索引名（即需要映射的表名），可通过访问http://10.16.15.8:9200/_plugin/head，了解珠江数码环境表命名方式、数据结构等
        String sparkSqlTable = "tmp"+System.currentTimeMillis(); // 映射为sparkSql中的表名
        DataFrame esDF = JavaEsSparkSQL.esDF(sqlContext, esTable, q, options);
        esDF.registerTempTable(sparkSqlTable);

        Row[] rows = sqlContext.sql(query.replace(esTable,sparkSqlTable)).collect();
        System.out.println(Arrays.toString(rows));

    }
}
