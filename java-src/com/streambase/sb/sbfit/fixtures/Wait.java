package com.streambase.sb.sbfit.fixtures;

import fit.Parse;
import fitlibrary.DoFixture;

public class Wait extends DoFixture {
	@Override
	public void doTable(Parse table) {
		super.doTable(table);
		
		try {
			Thread.sleep(new Long(args[0]));
		} catch (NumberFormatException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
