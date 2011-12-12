/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.streambase.sb.CompleteDataType;
import com.streambase.sb.DataType;
import com.streambase.sb.Schema;
import com.streambase.sb.StreamBaseException;
import com.streambase.sb.StreamBaseRuntimeException;
import com.streambase.sb.Timestamp;
import com.streambase.sb.Tuple;
import com.streambase.sb.TupleException;
import com.streambase.sb.unittest.JSONTupleMaker;

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
		if ( schema.getField(name).getDataType() == DataType.TUPLE ) {
			if (s==null || s.equals("null")) 
				return null;
			
			Schema fieldSchema = schema.getField(name).getSchema();
			try {
				return com.streambase.sb.unittest.CSVTupleMaker.MAKER.createTuple(fieldSchema, s);
			} catch(Throwable t) {
				return JSONTupleMaker.MAKER.createTuple(fieldSchema, s);
			}
		}
		else {
			return convert(schema.getField(name).getDataType(), s);
		}
	}

	public static List<?> convertList(CompleteDataType elementType, String s)
	throws StreamBaseException
	{
		if(s.equalsIgnoreCase("null")){
			return null;
		}
		else if(s.charAt(0)!='[' || s.charAt(s.length()-1)!=']'){
			throw new StreamBaseRuntimeException("Not a list");   
		}

		s = s.replaceAll("\"", "");
		s = s.substring(1, s.length()-1);
		if(s.length() == 0)
		{
			return Collections.EMPTY_LIST;
		}

		try
		{

			String[] valueList = s.split(",");

			// Get element type (I think this will always be a list)
			DataType eleDataType = elementType.getDataType();

			// Get the sub types
			CompleteDataType itemCompleteDataType = elementType.getElementType();
			DataType itemDataType = null;

			if (itemCompleteDataType != null) {
				itemDataType = itemCompleteDataType.getDataType();
			}
			else
			{
				itemDataType = eleDataType;
			}

			List<Object> list = new ArrayList<Object>();
			Schema schema = null;
			boolean treatAsTupleList = false;

			if (eleDataType == DataType.LIST && itemDataType == DataType.TUPLE)
			{
				// I am a list of tuples
				schema = itemCompleteDataType.getSchema();
				treatAsTupleList = true;
			}
			else if (eleDataType == DataType.TUPLE)
			{
				// I am a tuple type
				schema = elementType.getSchema();
				treatAsTupleList = true;
			}

			if(treatAsTupleList)
			{

				int row = 0;
				int fieldCount = schema.getFieldCount();                    
				int index = 0;
				while((index+fieldCount) <= valueList.length)
				{
					Tuple tuple = schema.createTuple();                        
					index = row * fieldCount;

					for(int i=0; i<fieldCount; i++,index++)
					{                               
						tuple.setField(i, convert(schema.getField(i).getDataType(), valueList[index]));
					}
					row++;                        
					list.add(tuple);
				}
			}
			else
			{
				for(int i=0; i<valueList.length; i++)
				{                        
					list.add(convert(itemDataType, valueList[i]));
				}
			}                
			return Collections.unmodifiableList(list);
		}
		catch(TupleException e)
		{
			throw new StreamBaseRuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public static boolean listsEqual(CompleteDataType elementType, String expectedValue, Object actualValue) throws StreamBaseException
	{
		if(expectedValue.equalsIgnoreCase("null")){
			return actualValue == null;
		}
		else if(expectedValue.charAt(0)!='[' || expectedValue.charAt(expectedValue.length()-1)!=']'){
			throw new StreamBaseRuntimeException("Expected value is not a list");   
		}

		try
		{
			DataType eleDataType = elementType.getDataType();

			expectedValue = expectedValue.substring(1, expectedValue.length()-1);
			String[] valueList = expectedValue.split(",");

			// Get the sub types
			CompleteDataType itemCompleteDataType = elementType.getElementType();
			DataType itemDataType = null;

			if (itemCompleteDataType != null) {
				itemDataType = itemCompleteDataType.getDataType();
			}
			else
			{
				itemDataType = eleDataType;
			}

			Schema schema = null;
			boolean treatAsTupleList = false;

			if (eleDataType == DataType.LIST && itemDataType == DataType.TUPLE)
			{
				// I am a list of tuples
				schema = itemCompleteDataType.getSchema();
				treatAsTupleList = true;
			}
			else if (eleDataType == DataType.TUPLE)
			{
				// I am a tuple type
				schema = elementType.getSchema();
				treatAsTupleList = true;
			}


			if(treatAsTupleList)
			{

				int row = 0;
				int fieldCount = schema.getFieldCount();                    
				int index = 0;

				Tuple tuple = null;

				while((index+fieldCount) <= valueList.length)
				{
					tuple = ((List<Tuple>)actualValue).get(row);                        
					for(int i=0; i<fieldCount; i++)
					{
						index = row * fieldCount + i;  
						Object actualFieldValue = tuple.getField(i);
						Object expectedFieldValue = convert(schema.getField(i).getDataType(), valueList[index]);
						if(actualFieldValue == null){
							if(expectedFieldValue !=null){
								return false;
							}
						}
						else if(!actualFieldValue.equals(expectedFieldValue)){
							return false;
						}
					}
					row++;                        
				}
			}
			else
			{
				for(int i=0; i<valueList.length; i++)
				{          
					Object actualFieldValue = ((List<?>)actualValue).get(i);
					Object expectedFieldValue = convert(itemDataType, valueList[i]);
					if(!actualFieldValue.equals(expectedFieldValue)){
						return false;
					}
				}
			}                
			return true;
		}
		catch(TupleException e)
		{
			throw new StreamBaseRuntimeException(e);
		}
	}

	public static Object convert(DataType type, String s) throws StreamBaseException {
		if(s.equalsIgnoreCase("null")){
			return null;
		}

		switch(type) {
		case BOOL: return Boolean.valueOf(s);
		case DOUBLE: return Double.valueOf(s);
		case INT: return Integer.valueOf(s);
		case LONG: return Long.valueOf(s);
		case STRING: return s;			
		case LIST: return s;
		case TIMESTAMP:
			return Timestamp.fromString(s);

		default:
			throw new StreamBaseRuntimeException("Not yet supported: " + type.toString() );
		}
	}
}