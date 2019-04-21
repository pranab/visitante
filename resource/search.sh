#!/bin/bash
#converts high cardinality categorical aatributes to numerical

PROJECT_HOME=/Users/pranab/Projects
VISITANTE_JAR_NAME=$PROJECT_HOME/bin/visitante/uber-visitante-spark-1.0.jar
MASTER=spark://akash:7077

case "$1" in

"genScore")	
	./search.py score $2 $3 $4 > $5
	cp $5 ./other/ncdg/
	ls -l ./other/ncdg/
	;;

"genClDist")	
	./search.py cdist $2  $3 > $4
	ls $4
	;;

"genRel")	
	./search.py rel  $2 $3 $4  > $5
	cp $5 ./input/ncdg/
	ls -l ./input/ncdg/
	;;

"ncdg")	
	echo "running NormDiscCumGain"
	CLASS_NAME=org.visitante.spark.search.NormDiscCumGain
	INPUT=file:///Users/pranab/Projects/bin/visitante/input/ncdg/*
	OUTPUT=file:///Users/pranab/Projects/bin/visitante/output/ncdg
	rm -rf ./output/ncdg
	rm -rf ./output/reaggr
	$SPARK_HOME/bin/spark-submit --class $CLASS_NAME   \
	--conf spark.ui.killEnabled=true --master $MASTER $VISITANTE_JAR_NAME  $INPUT $OUTPUT search.conf
	ls -l ./output/caen
	;;

*) 
	echo "unknown operation $1"
	;;

esac
