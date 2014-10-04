#!/bin/bash

if [ $# -lt 1 ]
then
        echo "Usage : $0 operation"
        exit
fi

case "$1" in
"genLogs")  
	# ./bounce.py <num_items> <num_sessions>
	echo  "generating logs and pushing to redis queue"
	./bounce.py genLogs $2 $3
    ;;
    
"readLogs")  
	# ./bounce.py readLogQueue
	echo  "reading logs from redis queue"
	./bounce.py readLogQueue
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
	storm  jar uber-visitante-1.0.jar  org.visitante.realtime.VisitTopology  bounce bounce.properties
	;;

"killTopology")
	echo  "killing visitante storm topology"
	storm  kill  bounce
	;;

"readStats")  
	# ./bounce.py readStatQueue
	echo  "reading stats from redis queue"
	./bounce.py readStatQueue
    ;;

*) 
	echo "unknown operation $1"
	;;

esac

