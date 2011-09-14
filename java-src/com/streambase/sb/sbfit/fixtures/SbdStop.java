/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import com.streambase.sb.sbfit.common.util.DebugLogging;
import fit.Parse;
import fitlibrary.SequenceFixture;

public class SbdStop extends SequenceFixture {
	private SbWithFixture with = null;

	public SbdStop() {
		with = new SbWithFixture(this, SbFixtureType.SbdStart);
	}

	public void doTable(Parse rows) {
		if (args.length != 1) {
			DebugLogging.getLogger().debugLog( "SbdStop requires one argument" );
			exception(rows, new IllegalArgumentException(
					"Usage: arg0: <alias>"));
		}

		try {
			DebugLogging.getLogger().debugLog( "Running sbd stop " + args[0] );
			with.stopSbd(args[0]);
		} catch (Exception e) {
			DebugLogging.getLogger().debugLog( "Runtime exception: " + e.toString() );
			throw new RuntimeException(e);
		}
	}
}
