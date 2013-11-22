This project contains the SbFit fixtures for creating FitNesse tests
for the StreamBase CEP platform.

To run on Linux, cd to the root of this project and run-sbfit.sh
To run on Windows, cd to the root of this project and run-sbfit.cmd

Then point a browser to http://<thismachine>:8080

There is a sample test suite there that illustrates the use of the 
SbFit fixtures.

There are tests in the included test suite that assume a MySQL
server running as configured using the StreamBase data-source
myDB. Modify ./sbd.sbconf to match
your database.

The tests assume a pre-created database called dbname and a
table named hayden with the schema as below:

create database dbname;
create table hayden (a int, b int, c double, d varchar(100), e varchar(100));

The master branch of SbFit works with StreamBase 7.2 and later -- it
relies on a version of the com.streambase.sb.jdbc.DataSourceInfo
interface that changed as of 7.2.0. There is a pre72 branch that works
with StreamBase 6.6-7.1.

SbFit starts its own instance of the StreamBase server (sbd) each time
a test (suite) is run -- it does not expect a StreamBase server to be
running separately.



