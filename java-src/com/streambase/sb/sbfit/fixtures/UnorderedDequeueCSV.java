package com.streambase.sb.sbfit.fixtures;

import java.io.File;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fit.ColumnFixture;
import fit.Parse;

public class UnorderedDequeueCSV extends ColumnFixture {
    private static final Logger logger = LoggerFactory.getLogger(UnorderedDequeueCSV.class);
    private SbWithFixture with = null;
    private String csvFileName;
    
    public UnorderedDequeueCSV() {
        with = new SbWithFixture(this, SbFixtureType.UnorderedDequeueCSV);
    }
    
    public void doRows(Parse rows) {
    	logger.info("UnorderedDequeueCSV: {}", Arrays.asList(args).toString());
    	
        try {
        	with.start();
        	
        	if(args.length != 2) {
        		throw new Exception(
                        "Incorrect arguments for UnorderedDequeueCSV, the correct arguments should be: \n"
                                + " 1. Alias (mytest) \n" + " 2. Stream to use \n");
        	}
        	

        	logger.debug("pwd is {}", new File(".").getAbsoluteFile());
        	
        	with.doDequeueArgs(rows, args);
            csvFileName = rows.parts.text();
            
            logger.info("csv file is {}", rows.parts.text());
            
            with.initBindings();
            with.unorderedDequeueCSV(rows, csvFileName);
        } catch (Throwable e) {
            logger.error("UnorderedDequeueCSV", e);
            exception(rows.parts, e);
        } finally {
            with.stop();
        }
    }
}
