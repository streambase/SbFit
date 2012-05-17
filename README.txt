This project contains the SbFit fixtures for creating FitNesse tests for the StreamBase CEP platform.

To run on Linux, cd to the root of this project and run-sbfit.sh
To run on Windows, cd to the root of this project and run-sbfit.cmd

Then point a browser to http://<thismachine>:8080

There is a sample test suite there that illustrates the use of the SbFit fixtures.

SbFit works with StreamBase 6.6 and later -- it relies on the existence of the SB Junit (SBServerManager) framework.

SbFit starts its own instance of the StreamBase server (sbd) each time a test (suite) is run -- it does not expect
a StreamBase server to be running separately. 

