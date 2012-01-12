#!/bin/sh

#
# This is the env var that sbfit uses to find the sbconf
#
export STREAMBASE_CONFIG=sbd.sbconf

FIT_PORT=8080
SBFIT=java-bin

SBUNIT_EXT=lib/sbunit-ext
SBUNIT_EXT_JARS=${SBUNIT_EXT}/build/sbunit-ext.jar:${SBUNIT_EXT}/lib/gson-1.7.1.jar

if [ `uname` == "CYGWIN_NT-5.1" ]
 then
  export STREAMBASE_HOME=`cygpath -ws "$STREAMBASE_HOME"`
fi

export CLASSPATH=$SBFIT:lib/fitnesse-20101101.jar:lib/fitlibrary.jar:$STREAMBASE_HOME/lib/sbclient.jar:$STREAMBASE_HOME/lib/junit.jar:$STREAMBASE_HOME/lib/sbtest-unit.jar:$STREAMBASE_HOME/lib/slf4j-api-1.6.1.jar:${SBUNIT_EXT_JARS}

if [ `uname` == "CYGWIN_NT-5.1" ]
 then
  export CLASSPATH=`cygpath -wp "$CLASSPATH"`
fi

echo Running ant build to pick up any java source changes
ant

mkdir -p build/sbars

java -Dlogback.configurationFile=sbfit-main-logback.xml fitnesseMain.FitNesseMain -e 0 -p $FIT_PORT
