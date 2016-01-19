#!/bin/bash

JAR_NAME=/home/pranab/Projects/visitante/target/visitante-1.0.jar
CLASS_NAME=org.visitante.basic.SessionEventDetector

echo "running mr"
IN_PATH=/user/pranab/sed/input
OUT_PATH=/user/pranab/sed/output
echo "input $IN_PATH output $OUT_PATH"
hadoop fs -rmr $OUT_PATH
echo "removed output dir $OUT_PATH"

hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=/home/pranab/Projects/bin/visitante/visitante.properties  $IN_PATH  $OUT_PATH
