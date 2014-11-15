#!/bin/bash

if [ $# -lt 1 ]
then
        echo "Usage : $0 operation"
        exit
fi

case "$1" in
"genLogs")  
	# ./mobile_app_usage.py <num_sessions>
	echo  "generating logs and pushing to redis queue"
	./mobile_app_usage.py genLogs $2 
	;;
    
"readAppLog")  
	# ./mobile_app_usage.py readAppLog
	echo  "reading logs from redis queue"
	./mobile_app_usage.py readAppLog
    ;;

"buildJar")
	echo  "building uber jar"
	ant -f build_storm.xml
	;;

"startStorm")
	echo  "starting storm"
	storm nimbus &
	sleep 5
	storm supervisor & 
	sleep 5
	storm ui &
	;;

"deployTopology")
	echo  "deploying visitante storm topology"
	storm  jar uber-visitante-1.0.jar  org.visitante.realtime.UniqueVisitorTopology  app_usage app_user.properties
	;;

"killTopology")
	echo  "killing visitante storm topology"
	storm  kill  app_usage
	;;

"readUserCount")  
	# ./mobile_app_usage.py readUserCount
	echo  "reading stats from redis queue"
	./mobile_app_usage.py readUserCount
    ;;

*) 
	echo "unknown operation $1"
	;;

esac

