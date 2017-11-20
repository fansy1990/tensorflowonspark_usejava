package demo_es_read;

import spark.engine.SparkEngine;

import java.io.IOException;

/**
 * Created by fansy on 2017/10/31.
 */
public class EsRead {
    public static void main(String[] args) throws IOException {
        String appName = "simple read es to hive";
        String[] arg = {
                "--name",appName,
                "--class","datasource.ReadES2Hive",
                "--executor-cores",SparkEngine.getExecutorCores(),
                "--num-executors",SparkEngine.getNumExecutors(),
                "--executor-memory",SparkEngine.getExecutorMemory(),
                "--jar", SparkEngine.getAlgorithmJar(),
                "--arg","mmconsume/payevents",
                "--arg","default.demo02",
                "--arg","10",
                "--arg",appName
        };

        String id = SparkEngine.runSpark(arg);
        System.out.println("result : " + id);
    }
}
