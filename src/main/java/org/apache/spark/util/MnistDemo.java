package org.apache.spark.util;

import java.io.File;
import java.io.IOException;

/**
 * Created by fansy on 2017/10/30.
 */
public class MnistDemo {
    public static void main(String[] args) throws IOException {
//        test1();
//        test2();
    test3();
//        test4();
    }


    private static void test4() throws IOException {
        String pyFileStr = "src/main/java/pyfiles/mnist_spark.py";
        File pyFile = new File(pyFileStr);
        System.out.println(pyFile.toURI().toString());
        String[] args = {
                "--primary-py-file",pyFile.toURI().toString(),
                "--archives","hdfs://s0:8020/user/root/Python.zip#Python",
                "--class","org.apache.spark.deploy.PythonRunner",
                "--executor-memory","720M",
                "--queue", "default",
                "--executor-cores","1",
                "--py-files",
                "hdfs://s0:8020/user/root/pyspark.zip,hdfs://s0:8020/user/root/py4j-0.8.2.1-src.zip",
                "--arg","--images",
                "--arg","hdfs://s0:8020/user/root/mnist_csv/train-features-part-00000",
                "--arg","--labels",
                "--arg","hdfs://s0:8020/user/root/mnist_csv/train-label-part-00000",
                "--arg","--mode",
                "--arg","train",
                "--arg","--model",
                "--arg","hdfs://s0:8020/tmp/mnist_model04"
        };
        String hadoopConfigFile = "./src/main/java/org/apache/spark/util/hadoop.properties";
        System.out.println(new File(".").getAbsoluteFile());
        String sparkConfigFile = "./src/main/java/org/apache/spark/util/spark.properties";
        int t = SparkUtilsTest.runSparkJob(hadoopConfigFile, sparkConfigFile, args);
        System.out.println("result : " + t);

    }

    private static void test3() throws IOException {
        String pyFileStr = "src/main/java/pyfiles/mnist_spark.py";
        File pyFile = new File(pyFileStr);
        System.out.println(pyFile.toURI().toString());
        String[] args = {
                "--primary-py-file",pyFile.toURI().toString(),
                "--archives","hdfs://hacluster/user/root/tipdm/Python.zip#Python",
                "--class","org.apache.spark.deploy.PythonRunner",
                "--executor-memory","15G",
                "--queue", "default",
                "--executor-cores","4",
                "--py-files",
                "hdfs://hacluster/user/root/tipdm/pyspark.zip,hdfs://hacluster/user/root/tipdm/py4j-0.8.2.1-src.zip",
                "--arg","--images",
                "--arg","hdfs://hacluster/tmp/mnist_csv/train/images",
                "--arg","--labels",
                "--arg","hdfs://hacluster/tmp/mnist_csv/train/labels",
                "--arg","--mode",
                "--arg","train",
                "--arg","--model",
                "--arg","hdfs://hacluster/tmp/mnist_model04"
        };
        String hadoopConfigFile = "./src/main/java/org/apache/spark/util/hadoop_hw.properties";
        System.out.println(new File(".").getAbsoluteFile());
        String sparkConfigFile = "./src/main/java/org/apache/spark/util/spark_hw.properties";
        int t = SparkUtilsTest.runSparkJob(hadoopConfigFile, sparkConfigFile, args);
        System.out.println("result : " + t);

    }

    /**
     * 数据建模 use mnist  csv data
     * @throws IOException
     */
    private static void test2() throws IOException {
        String pyFileStr = "src/main/java/pyfiles/mnist_spark.py";
        File pyFile = new File(pyFileStr);
        System.out.println(pyFile.toURI().toString());
        String[] args = {
                "--primary-py-file",pyFile.toURI().toString(),
                "--archives","hdfs://hacluster/user/root/tipdm/Python.zip#Python",
                "--class","org.apache.spark.deploy.PythonRunner",
                "--num-executors","4",// 这个参数没有用，需要在配置文件中配置
                "--executor-memory","2048M",
                "--queue", "default",
                "--executor-cores","2",
                "--py-files",
                "hdfs://hacluster/user/root/tipdm/pyspark.zip,hdfs://hacluster/user/root/tipdm/py4j-0.8.2.1-src.zip",
                "--arg","--images",
                "--arg","hdfs://hacluster/tmp/mnist_csv/train/images",
                "--arg","--labels",
                "--arg","hdfs://hacluster/tmp/mnist_csv/train/labels",
                "--arg","--mode",
                "--arg","train",
                "--arg","--model",
                "--arg","hdfs://hacluster/tmp/mnist_model"
        };
        String hadoopConfigFile = "./src/main/java/org/apache/spark/util/hadoop_hw.properties";
        System.out.println(new File(".").getAbsoluteFile());
        String sparkConfigFile = "./src/main/java/org/apache/spark/util/spark_hw.properties";
        int t = SparkUtilsTest.runSparkJob(hadoopConfigFile, sparkConfigFile, args);
        System.out.println("result : " + t);

    }

    private static void test1() throws IOException {
        String pyFileStr = "src/main/java/pyfiles/mnist_data_setup.py";
        File pyFile = new File(pyFileStr);
        System.out.println(pyFile.toURI().toString());
        String[] args = {
                "--primary-py-file",pyFile.toURI().toString(),
                "--archives","hdfs://hacluster/user/root/tipdm/Python.zip#Python,hdfs://hacluster/user/root/tipdm/mnist.zip#mnist",
                "--class","org.apache.spark.deploy.PythonRunner",
                "--num-executors","3",
                "--executor-memory","2048M",
                "--executor-cores","4",
                "--py-files",
                "hdfs://hacluster/user/root/tipdm/pyspark.zip,hdfs://hacluster/user/root/tipdm/py4j-0.8.2.1-src.zip",
                "--arg","--output",
                "--arg","hdfs://hacluster/tmp/mnist_csv",
                "--arg","--format",
                "--arg","csv"

        };
        String hadoopConfigFile = "./src/main/java/org/apache/spark/util/hadoop_hw.properties";
        System.out.println(new File(".").getAbsoluteFile());
        String sparkConfigFile = "./src/main/java/org/apache/spark/util/spark_hw.properties";

        int t = SparkUtilsTest.runSparkJob(hadoopConfigFile, sparkConfigFile, args);

        System.out.println("result : " + t);

    }
}
