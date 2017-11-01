package demo_iris_simple_net.demo_iris_simple_net.predict;

import org.apache.spark.util.SparkUtilsTest;

import java.io.File;
import java.io.IOException;

/**
 * Created by fansy on 2017/10/31.
 */
public class Simple_Iris_Predict_2_HDFS {
    public static void main(String[] args) throws IOException {
        String pyFileStr = "src/main/java/pyfiles/iris06_predict_2_hdfs.py";
        File pyFile = new File(pyFileStr);
        System.out.println(pyFile.toURI().toString());
        String[] arg = {
                "--primary-py-file",pyFile.toURI().toString(),
                "--archives","hdfs://s0:8020/user/root/Python.zip#Python",
                "--class","org.apache.spark.deploy.PythonRunner",
                "--executor-memory","720M",
                "--executor-cores","1",
                "--files",
                "hdfs://s0:8020/user/root/hive-site.xml",// 如果读取hive，则需要加上这个
                "--py-files",
                "hdfs://s0:8020/user/root/pyspark.zip,hdfs://s0:8020/user/root/py4j-0.8.2.1-src.zip",
                "--arg","--input",
                "--arg","default.t01",
                "--arg","--model",
                "--arg","hdfs://s0:8020/user/fansy/iris_model04/1509456351325",
                "--arg","--mode",
                "--arg","inference",
                "--arg","--batch_size",
                "--arg","20",
                "--arg","--output",
                "--arg","hdfs://s0:8020/user/fansy/iris_out01/"+System.currentTimeMillis()
        };
        String hadoopConfigFile = "./src/main/java/org/apache/spark/util/hadoop.properties";
        String sparkConfigFile = "./src/main/java/org/apache/spark/util/spark.properties";

        int t = SparkUtilsTest.runSparkJob(hadoopConfigFile, sparkConfigFile, arg);
        System.out.println("result : " + t);
    }
}
