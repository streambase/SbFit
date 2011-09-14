package com.streambase.sb.sbfit.common.util;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.sbd.SBDConf;
import com.streambase.sb.unittest.BaseTestEnvironment;

import java.io.*;

public class SbFixtureTestEnvironment extends BaseTestEnvironment {
	
	private String filename;
	
	public SbFixtureTestEnvironment( String filename ) {
		super();
		this.filename = filename;
		DebugLogging.getLogger().debugLog( "Using configuration file " + filename );
	}
	
	@Override
	public SBDConf getConf() throws StreamBaseException {
		return new SBDConf( new File( filename ) );
	}
}
