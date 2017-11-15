package org.apache.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static Configuration conf = null ;
    public  static Configuration getConf() throws IOException {
        if(conf == null ){
            conf  = new Configuration();
            Properties properties = new Properties();
            try {
                properties.load(new FileInputStream(HADOOP_PROPERTIES));
                String platform = properties.getProperty("platform");
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
//        Configuration.dumpConfiguration(conf,new FileWriter("./conf.out"));
//        System.exit(-1);
        return conf ;
    }

    /**
     * 添加资源
     * @param conf
     * @param platform
     */
    private static void handle(Configuration conf, String platform) throws MalformedURLException {
        File file = new File("./src/main/resources/"+platform);
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
    }

}
