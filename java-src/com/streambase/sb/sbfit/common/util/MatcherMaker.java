package com.streambase.sb.sbfit.common.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streambase.sb.Schema;
import com.streambase.sb.Tuple;
import com.streambase.sb.TupleException;
import com.streambase.sb.adapter.common.csv.CSVTupleReader;
import com.streambase.sbunit.ext.Matchers;
import com.streambase.sbunit.ext.TupleMatcher;
import com.streambase.sbunit.ext.matchers.FieldBasedTupleMatcher;

import fit.Parse;

public class MatcherMaker {
    private static final Logger logger = LoggerFactory.getLogger(MatcherMaker.class);

	public static List<TupleMatcher> makeMatchers(Schema s, Parse row, String[] fields) throws Throwable {
	    List<TupleMatcher> matchers = new ArrayList<TupleMatcher>();
	
	    while((row=row.more) != null) {
	    	Parse cell = row.parts;
	    	matchers.add(MatcherMaker.makeMatcher(s, cell, fields));
	    }
		return matchers;
	}

	public static List<TupleMatcher> makeMatchers(Schema s, Parse row, String csvFileName) throws Throwable {
	    List<TupleMatcher> matchers = new ArrayList<TupleMatcher>();
	    File file = new File(csvFileName);
	    
	    logger.info("makeMatchers: reading from {}", file.getAbsoluteFile());
	    
	    BufferedReader reader = new BufferedReader(new FileReader(file));
	    CSVTupleReader tupleReader = new CSVTupleReader(reader);

	    try {
	    	String [] header = tupleReader.readRecord();
	    	String [] line = null;
	    	
	    	logger.debug("headers: {}", Arrays.asList(header).toString());
	    	
	    	while((line=tupleReader.readRecord()) != null) {
		    	logger.debug("line: {}", Arrays.asList(line).toString());
	    		matchers.add(MatcherMaker.makeMatcher(s, line, header));
	    	}
	    }
	    finally {
	    	reader.close();
	    }
	    
		return matchers;
	}

	public static FieldBasedTupleMatcher makeMatcher(Schema s, Parse cell, String[] fields) throws TupleException {
		FieldBasedTupleMatcher m = Matchers.emptyFieldMatcher();
	
		for (int column = 0; column < fields.length; column++, cell = cell.more) {
			if (cell.text() == null || "null".equalsIgnoreCase(cell.text())) {
				m = m.requireNull(fields[column]);
			} else {
				Tuple t = s.createTuple();
				
				t.setField(fields[column], cell.text());
				m = m.require(fields[column], t.getField(fields[column]));
			}
		}
		return m;
	}

	public static FieldBasedTupleMatcher makeMatcher(Schema s, String [] row, String[] fields) throws TupleException {
		FieldBasedTupleMatcher m = Matchers.emptyFieldMatcher();
	
		for (int column = 0; column < fields.length; column++) {
			if (row[column] == null || "null".equalsIgnoreCase(row[column])) {
				m = m.requireNull(fields[column]);
			} else {
				Tuple t = s.createTuple();
				
				t.setField(fields[column], row[column]);
				m = m.require(fields[column], t.getField(fields[column]));
			}
		}
		return m;
	}
}
