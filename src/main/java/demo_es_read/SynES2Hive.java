package demo_es_read;

import org.apache.util.Utils;
import spark.engine.SparkEngine;

import java.io.IOException;

/**
 * Created by fansy on 2017/11/22.
 */
public class SynES2Hive {
    public static void main(String[] args) throws IOException {

        Utils.upload();

        String appName = "syn es to hive ";
        String esTable = "media-index/media";
//        String query = q1_q2("q1",esTable);
        String query = q1_q2("q2",esTable);

        String[] arg = {
                "--name",appName,
                "--class","datasource.SynES2Hive",
                "--executor-cores",SparkEngine.getExecutorCores(),
                "--num-executors",SparkEngine.getNumExecutors(),
                "--executor-memory",SparkEngine.getExecutorMemory(),
                "--jar", SparkEngine.getAlgorithmJar(),
                "--arg",esTable,
                "--arg",query,
                "--arg","default" ,
                "--arg","demo04",

                "--arg",appName
        };

        String id = SparkEngine.runSpark(arg);
        System.out.println("result : " + id);
    }

    private static String q1_q2(String q,String esTable){
        if("q1".equals(q)){
            return "select * from "+ esTable+
                    " where end_time < '2016-04-02 00:00:00' and end_time > '2016-04-01 00:00:00'";
        }
     return    "select * from "+ esTable+
                " where end_time < '2016-04-03 00:00:00' and end_time > '2016-04-02 00:00:00'";
    }
}
