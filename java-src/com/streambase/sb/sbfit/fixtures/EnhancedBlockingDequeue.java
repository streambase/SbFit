/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import fit.Parse;

public class EnhancedBlockingDequeue extends BlockingDequeue {	
	public void doRows(Parse rows) {
        try {
            rows = with.doArgs(rows,args);
            
            if (rows == null) {
            	return;
            }
            
 			with.initBindingsWithExclusions(rows.parts);			

			with.blockingDequeueWithExpects(rows);
        } catch (Throwable e){
        	exception(rows.parts,e);
        }
      }
}
