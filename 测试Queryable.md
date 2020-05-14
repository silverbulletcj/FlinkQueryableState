* 环境配置
  * Flink版本： 1.9.2
    * 将*flink-1.9.2/opt/flink-queryable-state-runtime.jar*拷贝到*flink-1.9.2/lib/*下
    * 在*flink-1.9.2/conf/flink-conf.yaml*中添加**queryable-state.enable: true**内容
    * 在*flink-1.9.2/conf/flink-conf.yaml*中添加**queryable-state.server.ports: 9067**内容
    * 在*flink-1.9.2/conf/flnk-conf.yaml*中添加**queryable-state.proxy.ports: 9096**内容
    * 更多flink-conf.yaml配置Queryable State内容，可参考[官方手册][https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/config.html#queryable-state]
  * Kafka版本：2.5.0
* 运行步骤
  * 在本地启动以`./bin/start-cluster.sh`命令启动flink
  * 将项目maven导入IDE
  * 控制台采用命令`mvn clean package -Pbuild-jar`打包得到JAR文件
  * 将target中的flink-java-project-0.1.jar提交给Flink运行
  * 启动test目录下的KafkaUtils以产生数据
  * 运行test目录下的Query即可以查看到当前sink的数据

