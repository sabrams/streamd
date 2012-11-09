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
    echo "    -plugins <path>   the path to the plugin directory."
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
            CONF=${VALUE}
            ;;
        -plugins)
            classpath ${VALUE}
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
if [ -z "$STREAMD_HOME" ]; then
    usage 
    echo "ERROR: STREAMD_HOME is not set"
    exit 1
fi

if [ -z "$CONF" ]; then 
    usage
    echo "ERROR: No configuration specified"
    exit 1
elif [ -e "$CONF" ]; then
    echo "Configuration file is: $CONF"
else  
    usage
    echo "ERROR: No configuration specified"
    exit 1
fi

if [ -n "$CLIENT" ] && [ -n "$DAEMON" ]; then
    usage
    echo "ERROR: Can not run client as a daemon"
    exit 1
fi

if [ -n "$CLIENT" ]; then
    JAVA_OPTS="-Done-jar.main.class=com.appendr.streamd.Driver ${JAVA_OPTS}"
fi


APP_ARGS=${CONF}

if [ -z ${DAEMON} ] && [ -z "$CLIENT" ]; then
    APP_ARGS="$APP_ARGS Xdaemon"
fi

JAVA_OPTS="-server -Xmx3g -Xms3g ${JAVA_OPTS}"
echo "---> running: java $JAVA_OPTS -Done-jar.class.path=$CLASSP -jar $STREAMD_HOME/streamd.one-jar.jar $APP_ARGS"
java $JAVA_OPTS $JMX -Done-jar.class.path=$CLASSP -jar $STREAMD_HOME/streamd.one-jar.jar $APP_ARGS

