package demo_es_read;

import spark.engine.SparkEngine;

import java.io.IOException;

/**
 * Created by fansy on 2017/11/22.
 */
public class EsReadInScala {
    public static void main(String[] args) throws IOException {
        String appName = "simple read es to hive use scala";
        String[] arg = {
                "--name",appName,
                "--class","datasource.ReadES2HiveWithScala",
                "--executor-cores",SparkEngine.getExecutorCores(),
                "--num-executors",SparkEngine.getNumExecutors(),
                "--executor-memory",SparkEngine.getExecutorMemory(),
                "--jar", SparkEngine.getAlgorithmJar(),
                "--arg","mmconsume/payevents",
                "--arg","default.demo03"+System.currentTimeMillis(),
                "--arg","10",
                "--arg",appName
        };

        String id = SparkEngine.runSpark(arg);
        System.out.println("result : " + id);
    }
}
