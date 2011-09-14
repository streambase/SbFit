package com.streambase.sb.sbfit.common.util;

import java.io.*;

public class DebugLogging {

	static private DebugLogging global;
	static public DebugLogging getLogger() {
		if ( global == null ) {
			global = new DebugLogging();
		}
		return global;
	}
	
	private PrintStream output; 
	private DebugLogging() {
		String dbg = System.getenv("STREAMBASE_DEBUG");
		if ( dbg != null && ( dbg.equals("true") || dbg.equals("yes") || dbg.equals("1") ) ) {
			output = System.out;
		}
		else {
			output = null;
		}
	}
	
	public void debugLog( String msg ) {
		if ( output != null ) {
			output.println( msg );
		}
	}
}
