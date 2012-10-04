#!/bin/sh

#---------------------------------#
# dynamically build the classpath #
#---------------------------------#
CLASSP=
for i in `ls ${STREAMD_PLUGINS}/*.jar`
do
  CLASSP=${CLASSP}\|${i}
done

CLASSP=${CLASSP}
for i in `ls ${STREAMD_PLUGINS}/*.xml`
do
  CLASSP=${CLASSP}\|${i}
done

echo 'classpath:' ${CLASSP} 
java -Done-jar.class.path=${CLASSP} -jar ${STREAMD_HOME}/streamd.one-jar.jar $*

#java -Xdebug -Xrunjdwp:transport=dt_socket,address=8998,server=y,suspend=y -Done-jar.class.path=${CLASSP} -jar ${STREAMD_HOME}/streamd.one-jar.jar $*
