package demo_iris_simple_net;

import org.apache.spark.util.SparkUtilsTest;

import java.io.File;
import java.io.IOException;

/**
 * Created by fansy on 2017/10/31.
 */
public class Simple_Iris_HDFS {
    public static void main(String[] args) throws IOException {
        String pyFileStr = "src/main/java/pyfiles/iris04_with_hdfs.py";
        File pyFile = new File(pyFileStr);
        System.out.println(pyFile.toURI().toString());
        String[] arg = {
                "--primary-py-file",pyFile.toURI().toString(),
                "--archives","hdfs://s0:8020/user/root/Python.zip#Python",
                "--class","org.apache.spark.deploy.PythonRunner",
                "--executor-memory","720M",
                "--executor-cores","1",
                "--py-files",
                "hdfs://s0:8020/user/root/pyspark.zip,hdfs://s0:8020/user/root/py4j-0.8.2.1-src.zip",
                "--arg","--input",
                "--arg","hdfs://s0:8020/user/root/iris01.csv",
                "--arg","--model",
                "--arg","hdfs://s0:8020/user/fansy/iris_model03/"+System.currentTimeMillis(),
                "--arg","--mode",
                "--arg","train",
                "--arg","--batch_size",
                "--arg","20"
        };
        String hadoopConfigFile = "./src/main/java/org/apache/spark/util/hadoop.properties";
        String sparkConfigFile = "./src/main/java/org/apache/spark/util/spark.properties";

        int t = SparkUtilsTest.runSparkJob(hadoopConfigFile, sparkConfigFile, arg);
        System.out.println("result : " + t);
    }
}
