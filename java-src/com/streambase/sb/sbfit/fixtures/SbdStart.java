/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import com.streambase.sb.sbfit.common.util.DebugLogging;
import fit.Parse;
import fitlibrary.SequenceFixture;

public class SbdStart extends SequenceFixture {
	private SbWithFixture with = null;
	
	public SbdStart() {
		with = new SbWithFixture(this,SbFixtureType.SbdStart);
    }

	public void doTable(Parse rows) {
    	if (args.length != 2) {
			DebugLogging.getLogger().debugLog( "SbdStart requires two arguments" );
    		exception(rows, new IllegalArgumentException("Usage: <alias> <app>"));
    	}

		try {
			with.startSbd(args[0], args[1]);
		} catch (NumberFormatException e) {
			DebugLogging.getLogger().debugLog( "NumberFormat exception: " + e.toString() );
			throw new RuntimeException(e);
		} catch (Exception e) {
			DebugLogging.getLogger().debugLog( "Runtime exception: " + e.toString() );
			throw new RuntimeException(e);
		}
   }
}
