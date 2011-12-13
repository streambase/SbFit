/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import java.lang.reflect.InvocationTargetException;

import com.streambase.sb.CompleteDataType;
import com.streambase.sb.Schema;
import com.streambase.sb.StreamBaseException;
import com.streambase.sb.StreamBaseRuntimeException;
import com.streambase.sb.Tuple;
import com.streambase.sb.TupleException;
import com.streambase.sb.TupleJSONUtil;

import fit.TypeAdapter;

public final class SbTypeAdapter extends TypeAdapter {
	private final SbWithFixture fixture;
	private Schema schema;
	private final String name;
	private boolean isEnqueue;

	public SbTypeAdapter(SbWithFixture fixture, String name, boolean isEnqueue) {
		this.fixture = fixture;
		this.schema = fixture.schema;
		this.name = name;
		this.isEnqueue = isEnqueue;
	}


	@Override
	public Object get() throws IllegalAccessException,
	InvocationTargetException {
		if (isEnqueue) {
			return super.get();
		} else {
			try {
				return fixture.pivot.getField(name);
			} catch(TupleException e) {
				throw new StreamBaseRuntimeException(e);
			}
		}
	}


	@Override
	public void set(Object value) throws Exception {
		if (isEnqueue) {
			fixture.pivot.setField(name, value);
		} else {
			// super.set(value);
		}
	}

	@Override
	public Object parse(String s) throws Exception {
		Schema.Field f = schema.getField(name);
		return convert(f, s);
	}
	
	public static Object convert(Schema.Field f, String s) throws Exception {
		try {
			return convertNaive(f.getCompleteDataType(), s);
		} catch (Exception t) {
			try {
				return TupleJSONUtil.jsonToFieldObject(f, s);
			} catch (Exception t2) {
				throw t;
			}
		}
	}

	private static Object convertNaive(CompleteDataType cdt, String s) throws StreamBaseException {
		Schema sch = new Schema("dummy", new Schema.Field("f", cdt));
		Tuple t = sch.createTuple();
		t.setField(0, s);
		return t.getField(0);	
	}
}