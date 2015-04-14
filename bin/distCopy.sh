baseDir=$(cd `dirname $0`; pwd)

if [ "$JAVA_HOME" != "" ]; then
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA=java
fi

Zk_TOOLS_LOG4J_PROP=$baseDir/../conf/log4j.properties
Zk_TOOLS_LOG_DIR=$baseDir/../logs
ZK_LIB=$baseDir/../lib/zookeeper-3.4.6.jar:$baseDir/../lib/slf4j-api-1.6.1.jar
CLASSPATH="$baseDir/../target/classes:$baseDir/../target/zkTools*.jar:$ZK_LIB:$CLASSPATH"
echo $CLASSPATH

"$JAVA" "-DzkTools.log.dir=${ZK_TOOLS_LOG_DIR}" "-DzkTools.root.logger=${ZK_TOOLS_LOG4J_PROP}" \
     -cp "$CLASSPATH" \
     org.mogujie.zookeeper.tools.DistCp "$@"
