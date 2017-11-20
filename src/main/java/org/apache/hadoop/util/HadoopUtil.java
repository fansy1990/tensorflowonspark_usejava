package org.apache.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.engine.SparkEngine;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Properties;

/**
 * Created by fansy on 2017/11/8.
 */
public class HadoopUtil {
    private static final Logger log = LoggerFactory.getLogger(HadoopUtil.class);
    private static final String HADOOP_PROPERTIES ="./src/main/resources/hadoop.properties";
//    private static final String HADOOP_PROPERTIES ="/hadoop.properties";

    private static String HADOOP_PLATFORM = null;

    private  static String getKey(String key){
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(new File(HADOOP_PROPERTIES)));
            return  properties.getProperty(key);
        }catch (Exception e){
            log.error("加载配置文件{}失败！",HADOOP_PLATFORM);
        }
        return null;

    }

    private static Configuration conf = null ;
    public  static Configuration getConf()  {
        if(conf == null ){
            conf  = new Configuration();
            try {
                String platform = getHadoopPlatform();
                switch (platform){
                    case "apache":
                    case "cdh":
                    case "hdp":
                    case "huawei":
                        handle(conf,platform);
                        break;
                    default:
                        log.error("不匹配的集群平台：{}",platform);

                }
            }catch (Exception e){
                e.printStackTrace();
                log.error("加载配置文件{}失败！",HADOOP_PROPERTIES);
            }
        }
        return conf ;
    }

    /**
     * 添加资源
     * @param conf
     * @param platform
     */
    private static void handle(Configuration conf, String platform) throws MalformedURLException {
        HADOOP_PLATFORM = platform;
        File file = new File(getRealPlatformPath());
        if(!file.exists() || !file.isDirectory()){
            log.error("路径{}不是目录或不存在！",file.getAbsolutePath());
            return ;
        }
        for(File f : file.listFiles()){
            if(f.getAbsolutePath().lastIndexOf("xml") != -1) {
                log.info("添加{}资源！",f.getName());

                conf.addResource(f.toURI().toURL());
            }else{
                log.info("资源{}不是以xml结尾！",f.getAbsolutePath());
            }
        }

        // special treat
        conf.set("yarn.application.classpath", SparkEngine.getYarnClasspath());
    }

    public static String getHadoopPlatform() {
        if(HADOOP_PLATFORM == null){
            HADOOP_PLATFORM = getKey("platform");
        }
        return HADOOP_PLATFORM;
    }
    private static String getRealPlatformPath(){
        return "./src/main/resources/"+getHadoopPlatform();
    }
}
