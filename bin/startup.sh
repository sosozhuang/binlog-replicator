#!/bin/bash 

current_path=`pwd`
case "`uname`" in
    Linux)
		bin_abs_path=$(readlink -f $(dirname $0))
		;;
	*)
		bin_abs_path=`cd $(dirname $0); pwd`
		;;
esac
base=${bin_abs_path}/..
canal_conf=$base/conf/replicator.properties
logback_configurationFile=$base/conf/logback.xml
export LANG=en_US.UTF-8
export BASE=$base

if [ -f $base/bin/canal.pid ] ; then
	echo "found canal.pid , Please run stop.sh first ,then startup.sh" 2>&2
    exit 1
fi

if [ ! -d $base/logs/canal ] ; then
	mkdir -p $base/logs/canal
fi

## set java path
if [ -z "$JAVA" ] ; then
  JAVA=$(which java)
fi

ALIBABA_JAVA="/usr/alibaba/java/bin/java"
TAOBAO_JAVA="/opt/taobao/java/bin/java"
if [ -z "$JAVA" ]; then
  if [ -f $ALIBABA_JAVA ] ; then
  	JAVA=$ALIBABA_JAVA
  elif [ -f $TAOBAO_JAVA ] ; then
  	JAVA=$TAOBAO_JAVA
  else
  	echo "Cannot find a Java JDK. Please set either set JAVA or put java (>=1.5) in your PATH." 2>&2
    exit 1
  fi
fi

case "$#" 
in
0 ) 
	;;
1 )	
	var=$*
	if [ -f $var ] ; then 
		canal_conf=$var
	else
		echo "THE PARAMETER IS NOT CORRECT.PLEASE CHECK AGAIN."
        exit
	fi;;
3 )
	var=$1
	if [ -f $var ] ; then
		canal_conf=$var
	else 
		if [ "$1" = "debug" ]; then
			DEBUG_PORT=$2
			JMX_HOSTNAME=$3
			DEBUG_SUSPEND="n"
#			JAVA_DEBUG_OPT="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=$DEBUG_PORT,server=y,suspend=$DEBUG_SUSPEND"
			JAVA_DEBUG_OPT="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$DEBUG_PORT -Dcom.sun.management.jmxremote.port=1099 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=$JMX_HOSTNAME"
		fi
     fi;;
* )
	echo "THE PARAMETERS MUST BE THREE OR LESS.PLEASE CHECK AGAIN."
	exit;;
esac

str=`file -L $JAVA | grep 64-bit`
if [ -n "$str" ]; then
	JAVA_OPTS="-server -Xms2g -Xmx2g -Xss1024k -XX:+AlwaysPreTouch -XX:MaxGCPauseMillis=100 -XX:+UseG1GC -XX:MaxTenuringThreshold=6 -XX:InitiatingHeapOccupancyPercent=75 -XX:+UnlockExperimentalVMOptions -XX:G1MaxNewSizePercent=50 -XX:+ParallelRefProcEnabled"
else
	JAVA_OPTS="-server -Xms512m -Xmx1024m -XX:NewSize=256m -XX:MaxNewSize=256m -XX:MaxPermSize=128m "
fi

JAVA_OPTS=" $JAVA_OPTS -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Dfile.encoding=UTF-8"
CANAL_OPTS="-DappName=replicator -Dlogback.configurationFile=$logback_configurationFile -Dcanal.conf=$canal_conf"

if [ -e $canal_conf -a -e $logback_configurationFile ]
then 
	
	for i in $base/lib/*;
		do CLASSPATH=$i:"$CLASSPATH";
	done
 	CLASSPATH="$base/conf:$CLASSPATH";
 	
 	echo "cd to $bin_abs_path for workaround relative path"
  	cd $bin_abs_path
 	
	echo LOG CONFIGURATION : $logback_configurationFile
	echo canal conf : $canal_conf 
	echo CLASSPATH :$CLASSPATH
	$JAVA $JAVA_OPTS $JAVA_DEBUG_OPT $CANAL_OPTS -classpath .:$CLASSPATH mysql.binlog.replicator.Replicator 1>>$base/logs/canal/canal.log 2>&1 &
	echo $! > $base/bin/canal.pid 
	
	echo "cd to $current_path for continue"
  	cd $current_path
else 
	echo "canal conf("$canal_conf") OR log configration file($logback_configurationFile) is not exist,please create then first!"
fi
