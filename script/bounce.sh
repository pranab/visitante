#!/bin/bash

if [ $# -lt 1 ]
then
        echo "Usage : $0 operation"
        exit
fi

case "$1" in
"genLogs")  
	# ./bounce.py <num_items> <num_sessions>
	echo  "generating events and pushing to redis queue"
	./bounce.py genLogs $2 $3
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

*) 
	echo "unknown operation $1"
	;;

esac

