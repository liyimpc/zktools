baseDir=$(cd `dirname $0`; pwd)

if [ "$JAVA_HOME" != "" ]; then
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA=java
fi

Zk_TOOLS_LOG4J_PROP=$baseDir/../src/main/resources/log4j.properties
Zk_TOOLS_LOG_DIR=$baseDir/../logs
CLASSPATH="$baseDir/../target/classes:$baseDir/../target/zkTools*.jar:$CLASSPATH"

"$JAVA" "-DzkTools.log.dir=${ZK_TOOLS_LOG_DIR}" "-DzkTools.root.logger=${ZK_TOOLS_LOG4J_PROP}" \
     -cp "$CLASSPATH" \
     org.mogujie.zookeeper.tools.SuperUser "$@"
