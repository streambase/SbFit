/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import fit.ColumnFixture;
import fit.Parse;

public class DefineVariable extends ColumnFixture {
	private SbWithFixture with = null;
	
	public DefineVariable() {
		with = new SbWithFixture(this,SbFixtureType.DefineVariable);
    }

	public void doRows(Parse rows) {
        try {
            rows = with.doArgs(rows,args);
            
            if (rows == null) {
            	return;
            }
            
 			with.defineVariable(rows);
        } catch (Throwable e){
        	exception(rows.parts,e);
        }
      }
}
