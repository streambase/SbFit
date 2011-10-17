package com.streambase.sb.sbfit.common.util;

import java.util.ArrayList;
import java.util.List;

import com.streambase.sb.Schema;
import com.streambase.sb.Tuple;
import com.streambase.sb.TupleException;
import com.streambase.sbunit.ext.Matchers;
import com.streambase.sbunit.ext.TupleMatcher;
import com.streambase.sbunit.ext.matchers.FieldBasedTupleMatcher;

import fit.Parse;

public class MatcherMaker {

	public static List<TupleMatcher> makeMatchers(Schema s, Parse row, String[] fields) throws Throwable {
	    List<TupleMatcher> matchers = new ArrayList<TupleMatcher>();
	
	    while((row=row.more) != null) {
	    	Parse cell = row.parts;
	    	matchers.add(MatcherMaker.makeMatcher(s, cell, fields));
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

}
