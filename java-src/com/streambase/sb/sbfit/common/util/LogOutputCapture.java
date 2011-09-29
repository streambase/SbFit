package com.streambase.sb.sbfit.common.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;


public class LogOutputCapture {
	
	public class LogReader
	{
		private long lineNumber = 0;
		private BufferedReader currentReader;
		
		void updateUnderlying(byte[] data) throws IOException
		{
			currentReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data)));
			for (long i = lineNumber;i > 0; i--)
			{
				currentReader.readLine();
			}
		}
		
		public String readLine() throws IOException
		{
			String readLine = currentReader.readLine();
			if (null != readLine) lineNumber++;
			return readLine;
		}
	}

	private static LogOutputCapture cache;
	
	private PrintStream origionalStdOut;
	private PrintStream origionalStdErr;
	
	private ByteArrayOutputStream redirectedFileOut;
	private ByteArrayOutputStream redirectedFileErr;
	
	private Map<String,LogReader> outReader = new HashMap<String, LogReader>();
	private Map<String,LogReader> errReader = new HashMap<String, LogReader>();
	
	public LogReader getOutReader(boolean newReader, String name) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = redirectedFileOut;
		byteArrayOutputStream.flush();
		if (newReader || !outReader.containsKey(name))
		{
			outReader.put(name,new LogReader());
		}
		LogReader logReader = outReader.get(name);
		logReader.updateUnderlying(byteArrayOutputStream.toByteArray());
		return logReader;
	}

	public LogReader getErrReader(boolean newReader,String name) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = redirectedFileErr;
		byteArrayOutputStream.flush();
		if (newReader || !errReader.containsKey(name))
		{
			errReader.put(name,new LogReader());
		}
		LogReader logReader = errReader.get(name);
		logReader.updateUnderlying(byteArrayOutputStream.toByteArray());
		return logReader;
	}

	public static LogOutputCapture getCapturer()
	{
		if  (cache == null)  cache = new LogOutputCapture();
		return cache;
	}
	
	private LogOutputCapture() {
	}

	public synchronized void captureOutput() throws Exception {
		if (redirectedFileOut != null || redirectedFileErr != null) 
			return;
		
		redirectedFileOut = new ByteArrayOutputStream();
		redirectedFileErr = new ByteArrayOutputStream(); 
		
		origionalStdErr = System.err;
		origionalStdOut = System.out;
		System.setOut(new PrintStream(new OutputerStream(origionalStdOut, redirectedFileOut)));
		System.setErr(new PrintStream(new OutputerStream(origionalStdErr, redirectedFileErr)));
	}

	public synchronized void reset() {
		System.setOut(origionalStdOut);
		System.setErr(origionalStdErr);
		redirectedFileOut = null;
		redirectedFileErr = null; 
		
		outReader.clear();
		errReader.clear();
	}
	
	private class OutputerStream extends OutputStream
	{
		private OutputStream os;
		private PrintStream origional;

		public OutputerStream(PrintStream origional, OutputStream os)
		{
			this.origional = origional;
			this.os = os;
		}
		
		@Override
		public void write(byte[] arg0) throws IOException {
			origional.write(arg0);
			os.write(arg0);
		}
		
		@Override
		public void write(byte[] arg0, int arg1, int arg2) throws IOException {
			origional.write(arg0, arg1, arg2);
			os.write(arg0, arg1, arg2);
		}

		@Override
		public void write(int c) throws IOException {
			origional.write(c);
			os.write(c);
		}
	}
}
