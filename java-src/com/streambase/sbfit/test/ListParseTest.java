package com.streambase.sbfit.test;


import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;

import com.streambase.sb.CompleteDataType;
import com.streambase.sb.DataType;
import com.streambase.sb.Schema;
import com.streambase.sb.Tuple;
import com.streambase.sb.sbfit.fixtures.SbTypeAdapter;

/**
 * Tests around the convertList method
 * @author c001352
 *
 */
public class ListParseTest extends TestCase {

	private List<?> boolList;
	private CompleteDataType boolListType;
	
	private List<?> intList;
	private CompleteDataType intListType;
	
	private List<?> floatList;
	private CompleteDataType floatListType;
	
	private List<?> tupleList;
	private CompleteDataType tupleListType;
	
	private Schema schema;
	
	@Before
	public void setUp() throws Exception {
		schema = new Schema("test", Arrays.asList(
				Schema.createField(DataType.STRING, "a"),
				Schema.createField(DataType.INT, "b"),
				Schema.createField(DataType.BOOL, "c")
		));
		
		boolListType = CompleteDataType.forList(CompleteDataType.forBool());
		boolList = SbTypeAdapter.convertList(boolListType, "[true,false,true]");
		
		intListType = CompleteDataType.forList(CompleteDataType.forInt());
		intList = SbTypeAdapter.convertList(intListType, "[1,2,3]");
		
		floatListType = CompleteDataType.forList(CompleteDataType.forDouble());
		floatList = SbTypeAdapter.convertList(floatListType, "[0.1,1.0,2.1]");
		
		tupleListType = CompleteDataType.forList(CompleteDataType.forTuple(schema));
		tupleList = SbTypeAdapter.convertList(tupleListType, "[hello,1,true,world,2,false]");
		
	}
	
	@Test
	public void testLists() throws Exception
	{
		Tuple t1;
		Tuple t2;
		
		assertEquals(boolList, Arrays.asList(true, false, true));
		
		assertEquals(intList, Arrays.asList(1, 2, 3));
		
		assertEquals(floatList, Arrays.asList(0.1, 1.0, 2.1));
		
		
		t1 = schema.createTuple();
		t1.setString(0, "hello");
		t1.setInt(1, 1);
		t1.setBoolean(2, true);
		
		t2 = schema.createTuple();
		t2.setString(0, "world");
		t2.setInt(1, 2);
		t2.setBoolean(2, false);
		assertEquals(tupleList, Arrays.asList(t1, t2));
		
		assertTrue(SbTypeAdapter.listsEqual(tupleListType, "[hello,1,true,world,2,false]", Arrays.asList(t1, t2)));
		
		assertTrue(SbTypeAdapter.listsEqual(boolListType, "[true,false,true]", Arrays.asList(true, false, true)));
		
		assertTrue(SbTypeAdapter.listsEqual(intListType, "[1,2,3]", Arrays.asList(1, 2, 3)));
		
		assertTrue(SbTypeAdapter.listsEqual(floatListType, "[0.1,1.0,2.1]", Arrays.asList(0.1, 1.0, 2.1)));
		
		
	}

}
