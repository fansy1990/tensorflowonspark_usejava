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
 * 用于测试sql是否可以运行
 * Created by fanzhe on 2017/11/22.
 */
public class TestDirectRead {
    public static void main(String[] args){

        String query = "select * from mediamatch limit 10";

        HashMap<String, String> options = new HashMap<String, String>();
        options.put("es.nodes", "10.16.15.8"); // ES环境IP之一
        options.put("es.port", "9200"); // ES环境端口
        options.put("es.read.metadata", "true");
        options.put("es.mapping.date.rich", "false"); //必须，否则日期字段转换报错

        SparkConf conf = new SparkConf();
        conf.set("spark.master", "spark://node3.zjsm.com:7077");
        conf.set("spark.app.name", "read es table ");
        conf.set("spark.executor.memory","4g"); // 建议不小于z"1g"
        conf.set("spark.executor.cores","4");
        conf.set("spark.cores.max","8");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        String q = "?q=*:*"; // 支持queryString语法的过滤（若不使用过滤器则无需修改）
        String esTable = "media-index/media"; // ES中的索引名（即需要映射的表名），可通过访问http://10.16.15.8:9200/_plugin/head，了解珠江数码环境表命名方式、数据结构等
        String sparkSqlTable = "mediamatch"; // 映射为sparkSql中的表名
        DataFrame esDF = JavaEsSparkSQL.esDF(sqlContext, esTable, q, options);
        esDF.registerTempTable(sparkSqlTable);

        Row[] rows = sqlContext.sql(query).collect();
        System.out.println(Arrays.toString(rows));

    }
}
