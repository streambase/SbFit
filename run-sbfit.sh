#!/bin/sh

#
# This is the env var that sbfit uses to find the sbconf
#
export STREAMBASE_CONFIG=sbd.sbconf

FIT_PORT=8080
SBFIT=java-bin

export CLASSPATH=$SBFIT:fitnesse-20101101.jar:fitlibrary.jar:$STREAMBASE_HOME/lib/sbclient.jar:$STREAMBASE_HOME/lib/junit.jar:$STREAMBASE_HOME/lib/sbtest-unit.jar:$STREAMBASE_HOME/lib/slf4j-api-1.6.1.jar

java -Dlogback.configurationFile=sbfit-main-logback.xml fitnesseMain.FitNesseMain -e 0 -p $FIT_PORT
