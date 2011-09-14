/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import fit.ColumnFixture;
import fit.Parse;

public class BlockingDequeue extends ColumnFixture {
	protected SbWithFixture with = null;

	public BlockingDequeue() {
		with = new SbWithFixture(this,SbFixtureType.BlockingDequeue);
    }
	
	public void doRows(Parse rows) {
        try {
            rows = with.doArgs(rows,args);
            
            if (rows == null) {
            	return;
            }
            
 			with.initBindings(rows.parts);			

			with.blockingDequeue(rows, false);
        } catch (Throwable e){
        	exception(rows.parts,e);
        }
      }

}
