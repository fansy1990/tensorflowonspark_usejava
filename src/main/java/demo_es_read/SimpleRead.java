package demo_es_read;

import org.apache.spark.util.SparkUtilsTest;
import spark.engine.SparkEngine;

import java.io.File;
import java.io.IOException;

/**
 * Created by fansy on 2017/10/31.
 */
public class SimpleRead {
    public static void main(String[] args) throws IOException {
        String appName = "simple wordcount";
        String[] arg = {
                "--name",appName,
                "--class","datasource.SimpleTest",
                "--executor-cores",SparkEngine.getExecutorCores(),
                "--num-executors",SparkEngine.getNumExecutors(),
                "--executor-memory",SparkEngine.getExecutorMemory(),
                "--jar", SparkEngine.getAlgorithmJar(),
                "--arg","/user/root/test.txt",
                "--arg","/tmp/"+System.currentTimeMillis(),
                "--arg",appName
        };

        String id = SparkEngine.runSpark(arg);
        System.out.println("result : " + id);
    }
}
