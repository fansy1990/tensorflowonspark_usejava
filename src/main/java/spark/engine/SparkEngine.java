package spark.engine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.HadoopUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Spark 引擎，用于运行Spark算法
 *  调用自动匹配集群参数的HadoopUtils工具类
 * Created by fanzhe on 2017/11/17.
 */
public class SparkEngine {
    private static final Logger log = LoggerFactory.getLogger(SparkEngine.class);
    private static  String SPARK_PROPERTIES = "spark.properties";
    private static SparkConf sparkConf = null;
    private static String ALGORITHM_JAR = null ;
    private static String YARN_CLASSPATH = null;

    private static String combinePlatform_SparkConfigFile(){
        return "./src/main/resources/"+HadoopUtil.getHadoopPlatform()+"/"+SPARK_PROPERTIES;
    }

    private  static String getKey(String key){
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(new File(combinePlatform_SparkConfigFile())));
            return  properties.getProperty(key);
        }catch (Exception e){
            log.error("加载配置文件{}失败！",combinePlatform_SparkConfigFile());
        }
        return null;

    }

    public static SparkConf getSparkConf()  {
        if(sparkConf == null){
            sparkConf  = new SparkConf();
            updateConf(sparkConf,combinePlatform_SparkConfigFile());
        }
        return sparkConf;
    }


    private static void updateConf(SparkConf conf,String configFile ) {
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(new File(configFile)));
            for (final String name : properties.stringPropertyNames()) {
                conf.set(name, properties.getProperty(name));
            }
        }catch (IOException e){
            log.error("spark config file :{} can not be found !",configFile);
        }
    }


    public static String runSpark(String[] args) throws IOException {
        Configuration conf = HadoopUtil.getConf();
        StringBuffer buff = new StringBuffer();
        for(String arg:args){
            buff.append(arg).append(",");
        }
        log.info("runSpark args:"+buff.toString());
        ApplicationId appId = null;
        try {
            System.setProperty("SPARK_YARN_MODE", "true");
            SparkConf sparkConf = getSparkConf();

            ClientArguments cArgs = new ClientArguments(args, sparkConf);

            Client client = new Client(cArgs, conf, sparkConf);
            // 调用Spark
            try{
                appId = client.submitApplication();
            }catch(Throwable e){
                e.printStackTrace();
                // 清空临时文件
                cleanupStagingDir(conf, appId);
                //  返回null
                return null;
            }
            return appId.toString();
        } catch (Exception e) {
            e.printStackTrace();
            // 清空临时文件
            cleanupStagingDir(conf ,appId);
            return null;
        }
    }
    public static void cleanupStagingDir(Configuration conf ,ApplicationId appId) {
        String appStagingDir = Client.SPARK_STAGING() + Path.SEPARATOR + appId.toString();

        try {
            Path stagingDirPath = new Path(appStagingDir);
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(stagingDirPath)) {
                log.info("Deleting staging directory " + stagingDirPath);
                fs.delete(stagingDirPath, true);
            }
        } catch (IOException e) {
            log.warn("Failed to cleanup staging dir " + appStagingDir, e);
        }
    }
    public static void cleanupStagingDir(Configuration conf ,String appId) {
        cleanupStagingDir(conf, ConverterUtils
                .toApplicationId(appId));
    }


    public static String getAlgorithmJar() {
        if(ALGORITHM_JAR == null ){
            ALGORITHM_JAR = getKey("spark.jar");
        }
        return ALGORITHM_JAR;
    }

    public static String getYarnClasspath() {
        if(YARN_CLASSPATH ==null ){
            YARN_CLASSPATH = getKey("spark.yarn.appMasterEnv.SPARK_DIST_CLASSPATH");
        }
        return YARN_CLASSPATH;
    }

    public static String getNumExecutors() {
        if(NUM_EXECUTORS == null){
            NUM_EXECUTORS = getKey("spark.num.executors");
        }
        return NUM_EXECUTORS;
    }

    private static  String NUM_EXECUTORS= null;
    private static String EXECUTOR_MEMORY = null ;
    private static String EXECUTOR_CORES = null ;

    public static String getExecutorMemory() {
        if(EXECUTOR_MEMORY == null){
            EXECUTOR_MEMORY = getKey("spark.executor.memory");
        }
        return EXECUTOR_MEMORY;
    }
    public static String getExecutorCores() {
        if(EXECUTOR_CORES == null){
            EXECUTOR_CORES = getKey("spark.executor.cores");
        }
        return EXECUTOR_CORES;
    }
}
