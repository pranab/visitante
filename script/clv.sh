#!/bin/bash

JAR_NAME=/home/pranab/Projects/visitante/target/visitante-1.0.jar
CHOMBO_JAR_NAME=/home/pranab/Projects/chombo/target/chombo-1.0.jar
PROP_FILE=/home/pranab/Projects/bin/visitante/visitante.properties
HDFS_BASE_DIR=/user/pranab/clv

case "$1" in

"genInp")
	 ./purchase.py createOrders $2 $3 $4 $5 > $6
	 ls -l $6
;;

"copyInp")
	hadoop fs -rm $HDFS_BASE_DIR/input/$2
	hadoop fs -put $2 $HDFS_BASE_DIR/input
	hadoop fs -ls $HDFS_BASE_DIR/input
;;

"transFreq")
	echo "running MR TransactionFrequencyRecencyValue"
	CLASS_NAME= org.visitante.customer.TransactionFrequencyRecencyValue
	IN_PATH=$HDFS_BASE_DIR/input
	OUT_PATH=$HDFS_BASE_DIR/freq
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/freq/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/freq/_SUCCESS
	hadoop fs -ls $HDFS_BASE_DIR/freq
;;

"recencyScore")
	echo "running MR TransactionFrequencyRecencyValue"
	CLASS_NAME= org.visitante.customer.TransactionRecencyScore
	IN_PATH=$HDFS_BASE_DIR/freq
	OUT_PATH=$HDFS_BASE_DIR/recency
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/recency/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/recency/_SUCCESS
	hadoop fs -ls $HDFS_BASE_DIR/recency
;;

"norm")
	echo "running MR Normalizer"
	CLASS_NAME=  org.chombo.mr.Normalizer
	IN_PATH=$HDFS_BASE_DIR/recency
	OUT_PATH=$HDFS_BASE_DIR/norm
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $CHOMBO_JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -ls $HDFS_BASE_DIR/norm
;;

"lifeTimeValue")
	echo "running MR WeightedAverage"
	CLASS_NAME=  org.chombo.mr.WeightedAverage
	IN_PATH=$HDFS_BASE_DIR/norm
	OUT_PATH=$HDFS_BASE_DIR/value
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $CHOMBO_JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -ls $HDFS_BASE_DIR/value
;;


*) 
	echo "unknown operation $1"
	;;

esac
