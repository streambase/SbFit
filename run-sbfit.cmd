@rem This is the env var that sbfit uses to find the sbconf
set STREAMBASE_CONFIG=sbd.sbconf

set FIT_PORT=8080
set SBFIT=java-bin

set SBUNIT_EXT=lib\sbunit-ext
set SBUNIT_EXT_JARS=%SBUNIT_EXT%\build\sbunit-ext.jar;%SBUNIT_EXT%\lib\gson-1.7.1.jar

echo Running ant build to pick up any java source changes
call ant

mkdir build\sbars

set CLASSPATH="%SBFIT%;lib\fitnesse-20101101.jar;lib\fitlibrary.jar;%STREAMBASE_HOME%\lib\sbclient.jar;%STREAMBASE_HOME%\lib\junit.jar;%STREAMBASE_HOME%;lib\sbtest-unit.jar;%STREAMBASE_HOME%\lib\slf4j-api-1.6.1.jar;%SBUNIT_EXT_JARS%"

java -Dlogback.configurationFile=sbfit-main-logback.xml fitnesseMain.FitNesseMain -e 0 -p %FIT_PORT%
