package org.apache.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.HadoopUtil;

import java.io.IOException;

/**
 * 伪造用户，上传到其文件夹下面
 * 上传jar包到hdfs
 * Created by fanzhe on 2017/11/22.
 */
public class Utils {

    public static void upload() throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        FileSystem fs = FileSystem.get(HadoopUtil.getConf());
        fs.copyFromLocalFile(false,true,
                new Path("/Users/fanzhe/projects/fansy_githubs/" +
                        "tensorflowonspark_usejava/out/artifacts/es/es.jar"),
                new Path("/user/root/"));

    }

    public static void main(String[] args) throws IOException {
        upload();
    }
}
