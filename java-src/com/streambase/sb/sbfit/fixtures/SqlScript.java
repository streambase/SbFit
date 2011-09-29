package com.streambase.sb.sbfit.fixtures;

import java.io.File;

import com.streambase.sb.jdbc.DataSourceInfo;
import com.streambase.sb.sbd.ConfFile;
import com.streambase.sb.sbd.SBDConf;
import com.streambase.sb.sbfit.common.util.SBARCache;

import fit.ColumnFixture;
import fit.Parse;

public class SqlScript extends ColumnFixture {
	@Override
	public void doRows(Parse rows) {
		if(args.length != 1) {
			exception(rows.parts, new Exception("Missing data-source name (from sbconf) is a required argument"));
			return;
		}
		
		
	}
	
	
	private DataSourceInfo getDataSource(String dsName) {
		File sbconfFile = new File(SBARCache.getSBConfFileName());
//		SBDConf conf = new SBDConf()
		return null;
	}


}
