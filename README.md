
Java 调用
TensorFlow
TensorFlowOnSpark
SparkOnYARN

相关zip包，如Python.zip ，可参考http://blog.csdn.net/fansy1990/article/details/78370648


## Spark read Elasticsearch Data

1. 拷贝：

elasticsearch-1.7.4.jar
elasticsearch-spark_2.10-2.3.2.jar
jar包到 集群各个子节点 /opt/es_jars ;

2. 修改spark.properties 文件中的spark.yarn.appMasterEnv.SPARK_DIST_CLASSPATH
 参数，添加  :/opt/es_jars/*

3. 如果要写入Hive，那么需要：
  spark.yarn.appMasterEnv.SPARK_DIST_CLASSPATH
  添加Hive相关jar包路径，同时还要其配置文件路径；


## # 同步数据
1. 加载部分ES数据（根据时间戳），判断Hive表是否存在，如果存在，则插入数据，否则新建表数据