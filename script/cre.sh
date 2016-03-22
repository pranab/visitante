#!/bin/bash

JAR_NAME=/home/pranab/Projects/visitante/target/visitante-1.0.jar
CHOMBO_JAR_NAME=/home/pranab/Projects/chombo/target/chombo-1.0.jar
PROP_FILE=/home/pranab/Projects/bin/visitante/visitante.properties
HDFS_BASE_DIR=/user/pranab/cre

case "$1" in

"genInp")
	 ./visit.py  $2 $3 $4 $5 > $6
	 ls -l $6
;;

"copyInp")
	hadoop fs -rm $HDFS_BASE_DIR/input/$2
	hadoop fs -put $2 $HDFS_BASE_DIR/input
	hadoop fs -ls $HDFS_BASE_DIR/input
;;


"timeGap")
	echo "running MR TimeGapSequenceGenerator for time gap generation"
	CLASS_NAME=org.chombo.mr.TimeGapSequenceGenerator
	IN_PATH=$HDFS_BASE_DIR/input
	OUT_PATH=$HDFS_BASE_DIR/tgap
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $CHOMBO_JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/tgap/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/tgap/_SUCCESS
	hadoop fs -ls $HDFS_BASE_DIR/tgap
;;

"timeGapStats")
	echo "running MR NumericalAttrStats for time gap stats"
	CLASS_NAME=org.chombo.mr.NumericalAttrStats
	IN_PATH=$HDFS_BASE_DIR/tgap
	OUT_PATH=$HDFS_BASE_DIR/tgstats
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $CHOMBO_JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/tgstats/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/tgstats/_SUCCESS
	hadoop fs -ls $HDFS_BASE_DIR/tgstats
;;

"lifeCycle")
	echo "running MR LifeCycleFinder to find life cycles"
	CLASS_NAME=org.visitante.customer.LifeCycleFinder
	IN_PATH=$HDFS_BASE_DIR/tgap
	OUT_PATH=$HDFS_BASE_DIR/lcycles
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/lcycles/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/lcycles/_SUCCESS
	hadoop fs -ls $HDFS_BASE_DIR/lcycles
;;

"lifeCycleStats")
	echo "running MR NumericalAttrStats for life cycle stats"
	CLASS_NAME=org.chombo.mr.NumericalAttrStats
	IN_PATH=$HDFS_BASE_DIR/lcycles
	OUT_PATH=$HDFS_BASE_DIR/output
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $CHOMBO_JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/output/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/output/_SUCCESS
	hadoop fs -ls $HDFS_BASE_DIR/output
;;


*) 
	echo "unknown operation $1"
	;;

esac

