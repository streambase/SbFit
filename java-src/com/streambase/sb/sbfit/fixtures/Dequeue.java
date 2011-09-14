/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import com.streambase.sb.sbfit.common.util.DebugLogging;
import fit.ColumnFixture;
import fit.Parse;

public class Dequeue extends ColumnFixture {
	private SbWithFixture with = null;
	
	public Dequeue() {
		with = new SbWithFixture(this,SbFixtureType.Dequeue);
    }

	public void doRows(Parse rows) {
        try {
            rows = with.doArgs(rows,args);
            
            if (rows == null) {
            	return;
            }
            
 			with.initBindings(rows.parts);			

			with.dequeue(rows);
        } catch (Throwable e){
			DebugLogging.getLogger().debugLog( "Dequeue exception: " + e );
        	exception(rows.parts,e);
        }
      }
}
