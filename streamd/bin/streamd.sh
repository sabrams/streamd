#!/bin/bash

function usage()
{
    echo "--------------------------"
    echo "     _|_ _ _ _  _  _|"
    echo "    _)|_| (-(_||||(_|"
    echo "        stream daemon ~~~"
    echo "--------------------------"
    echo "streamd.sh"
    echo "    -help | -h        prints this message."
    echo "    -debug   <port>   run in debug mode, specify port."
    echo "    -jmx     <port>   open JMX port, specify port."
    echo "    -conf    <file>   run with the specified configuration file."
    echo "    -daemon           run as a daemon."
    echo "    -client           run the client driver."
}

CLASSP=

function classpath()
{
    for i in `ls $1/*.jar`
    do
      CLASSP=${CLASSP}\|${i}
    done
}

CONF=
DAEMON=
CLIENT=
JMX=
NODE=
while [ "$1" != "" ]; do
    PARAM=`echo $1 | awk -F= '{print $1}'`
    VALUE=`echo $1 | awk -F= '{print $2}'`
    case $PARAM in
        -h | --help)
            usage
            exit
            ;;
        -debug)
            JAVA_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,address=${VALUE},server=y ${JAVA_OPTS}"
            ;;
        -jmx)
            JMX="-Dcom.sun.management.jmxremote.port=${VALUE} -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
            ;;
        -conf)
            CONF=${STREAMD_HOME}/conf/${VALUE}
            ;;
        -client)
            CLIENT="1"
            ;;
        -daemon)
            DAEMON="1"
            ;;
        *)
            echo "ERROR: unknown parameter \"$PARAM\""
            usage
            exit 1
            ;;
    esac
    shift
done

# DO CHECKS
if [ -z "${STREAMD_HOME}" ]; then
    usage 
    echo "ERROR: STREAMD_HOME is not set"
    exit 1
fi

if [ -z "${CONF}" ]; then
    usage
    echo "ERROR: No configuration specified"
    exit 1
elif [ -e "${CONF}" ]; then
    echo "Configuration file is: ${CONF}"
else  
    usage
    echo "ERROR: No configuration specified"
    exit 1
fi

if [ -z "${CLIENT}" ]; then
    filename=$(basename "${CONF}")
    NODE="${filename%.*}"
fi

if [ -n "${CLIENT}" ] && [ -n "${DAEMON}" ]; then
    usage
    echo "ERROR: Can not run client as a daemon"
    exit 1
fi

if [ -z "${NODE}" ] && [ -z "${CLIENT}" ]; then
    usage
    echo "ERROR: No -node=<name> specified"
    exit 1
else
    echo "Node Name is: ${NODE}"
    JAVA_OPTS="-Dstreamd.nodeId=${NODE} \
    -Dlogback.configurationFile=${STREAMD_HOME}/conf/logback.xml \
    -Dlog4j.configuration=file://${STREAMD_HOME}/conf/log4j.xml \
    -Dstreamd.cep.configuration=file://${STREAMD_HOME}/conf/esper.default.xml ${JAVA_OPTS}"
fi

if [ -n "${CLIENT}" ]; then
    JAVA_OPTS="-Done-jar.main.class=com.appendr.streamd.Driver ${JAVA_OPTS}"
fi

APP_ARGS=${CONF}

if [ -z ${DAEMON} ] && [ -z "${CLIENT}" ]; then
    APP_ARGS="$APP_ARGS Xdaemon"
fi

classpath ${STREAMD_HOME}/lib
JAVA_OPTS="-server -Xmx1g -Xms1g $JAVA_OPTS"
echo "---> running: java $JAVA_OPTS -Done-jar.class.path=$CLASSP -jar $STREAMD_HOME/streamd.one-jar.jar $APP_ARGS"
java $JAVA_OPTS $JMX -Done-jar.class.path=$CLASSP -jar $STREAMD_HOME/streamd.one-jar.jar $APP_ARGS
