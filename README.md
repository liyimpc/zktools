
### 目标
该项目主要为了实现多个zookeeper迁移、资源隔离、以及监控等功能

### 功能
1. zookeeper super user
2. zookeeper distCopy
3. zookeeper ACL
4. zookeeper监控

### 使用
1. 首先编译: mvn clean install -DskipTests
2. distCopy使用: sh distCopy.sh -from 10.15.8.24:9261 -to 127.0.0.1:2181 [watch] [-timeout time]
3. superUser使用: </br> 
  3.1 sh superUser.sh super:guangming </br>
  3.2 产生加密后的密文: super:V56e0FnimNzScgvwtE1Jy1uwlIc= </br>
  3.3 修改Zookeeper的启动脚本zkServer.sh，在start）附近加入以下配置：
-Dzookeeper.DigestAuthenticationProvider.superDigest=super:V56e0FnimNzScgvwtE1Jy1uwlIc= </br>
  3.4 启动zookeeper，通过zkCli.sh进入命令行，输入addauth digest super:guangming </br>
  3.5 addauth命令只在当前会话有效，下次开启super user，需要重新输入addauth digest super:guangming </br>
