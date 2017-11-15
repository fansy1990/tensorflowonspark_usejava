package org.apache.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * 自动加载配置文件
 * Created by fansy on 2017/11/7.
 */
public class HadoopSparkUtils {
    private final static Logger logger = LoggerFactory.getLogger(HadoopSparkUtils.class);
    // 配置文件目录
    private static final String HDFS_TMP_FILE="/tmp/hadoop_spark_utils.check.file";
    private static final String HADOOP_CONF_DIR = "/huawei";
    private static final String SPARK_PROPERTIES="/huawei/spark.properties";
    // 配置文件
    private static final String[] configFiles = {
      "/core-site.xml",
            "/hdfs-site.xml",
            "/yarn-site.xml",
            "/hive-site.xml",
            "/mapred-site.xml"
    };


    private static Configuration conf  = null;

    /**
     * 获取Configuration配置
     * @return
     */
    public static Configuration getConf(){
        if(conf == null){
            conf = new Configuration();
            for(String file : configFiles){
                conf.addResource(HADOOP_CONF_DIR + file);
            }
        }
        return conf;
    }
    private static SparkConf sparkConf = null;
    public static SparkConf getSparkConf() throws IOException {
        if(sparkConf == null){
            sparkConf  = new SparkConf();
            updateConf(sparkConf,SPARK_PROPERTIES);
        }
        return sparkConf;
    }

    private static void updateConf(SparkConf conf,String configFile )throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(new File(configFile)));
        for(final String name: properties.stringPropertyNames()) {
            conf.set(name,properties.getProperty(name));
        }
    }

    public static String runSpark(String[] args) throws IOException {
        StringBuffer buff = new StringBuffer();
        for(String arg:args){
            buff.append(arg).append(",");
        }
        logger.info("runSpark args:"+buff.toString());
        ApplicationId appId = null;
        try {
            System.setProperty("SPARK_YARN_MODE", "true");
            SparkConf sparkConf = getSparkConf();
            ClientArguments cArgs = new ClientArguments(args, sparkConf);
            Client client = new Client(cArgs, getConf(), sparkConf);
            // 调用Spark
            try{
                appId = client.submitApplication();
            }catch(Exception e){
                return check(appId,e);
            }
            // 正常返回：
            logger.info("提交正常，APPID：{}", appId);
            return appId.toString();
        } catch (Exception e) {
            return check(appId, e);
        }
    }

    /**
     * 检查集群是否异常
     */
    private static String check(ApplicationId appId,Exception e) throws IOException {
        if(appId != null) {
            cleanupStagingDir(getConf(), appId);
        }
        logger.error("提交任务出错, 执行检查 ...");
        // 执行检查。。。
        logger.info("检查是否可以上传数据：");
        FileSystem fs = FileSystem.get(getConf());
        fs.copyFromLocalFile(false,true,new Path(SPARK_PROPERTIES),new Path(HDFS_TMP_FILE));
        if(fs.exists(new Path(HDFS_TMP_FILE))){
            logger.info("上传数据失败，请检查HDFS连接");
        }else{
            logger.info("可以上传数据");
        }
        logger.info("检查是否可以提交任务: ");

        fs.delete(new Path(HDFS_TMP_FILE),true);
        logger.error("错误日志：{}",e);
        return null;
    }

    public static void cleanupStagingDir(Configuration conf ,ApplicationId appId) {
        String appStagingDir = Client.SPARK_STAGING() + Path.SEPARATOR + appId.toString();

        try {
            Path stagingDirPath = new Path(appStagingDir);
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(stagingDirPath)) {
                logger.info("Deleting staging directory " + stagingDirPath);
                fs.delete(stagingDirPath, true);
            }
        } catch (IOException e) {
            logger.warn("Failed to cleanup staging dir " + appStagingDir, e);
        }
    }
    public static void cleanupStagingDir(Configuration conf ,String appId) {
        cleanupStagingDir(conf, ConverterUtils
                .toApplicationId(appId));
    }


    public static void main(String[] args) throws IOException {
        HadoopSparkUtils.runSpark(args);
    }
}
