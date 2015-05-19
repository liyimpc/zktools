
### 目标
该项目主要为了实现多个zookeeper迁移、资源隔离、以及监控等功能

### 功能
1. zookeeper super user
2. zookeeper distCopy
3. zookeeper ACL
4. zookeeper监控

### 使用
1. 首先编译: mvn clean install -DskipTests
2. distCopy使用: sh distCopy.sh -from host:port -to host:port -srcpath path1 [-despath path2] [watch] [-timeout time]  
   比如 sh disCopy.sh -from 10.15.8.24:2181 -to 10.15.8.27:2181 -srcpath /hbase -despath /bda/hbase/hbase1  
   将拷贝10.15.8.24这台zk /hbase下的路径到10.15.8.27这台zk /bda/hbase/hbase1下  
   如果不设置[despath], 则默认拷贝到srcpath同样路径下
3. superUser使用:  
  3.1） sh superUser.sh super:guangming  
  3.2） 产生加密后的密文: super:V56e0FnimNzScgvwtE1Jy1uwlIc=  
  3.3） 修改Zookeeper的启动脚本zkServer.sh，在start）附近加入以下配置:  
-Dzookeeper.DigestAuthenticationProvider.superDigest=super:V56e0FnimNzScgvwtE1Jy1uwlIc=  
  3.4） 启动zookeeper，通过zkCli.sh进入命令行，输入addauth digest super:guangming  
  3.5） addauth命令只在当前会话有效，下次开启super user，需要重新输入addauth digest super:guangming  
