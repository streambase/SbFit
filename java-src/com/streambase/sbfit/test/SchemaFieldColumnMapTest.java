package com.streambase.sbfit.test;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.streambase.sb.DataType;
import com.streambase.sb.Schema;
import com.streambase.sb.sbfit.common.util.SchemaFieldColumnMapper;

public class SchemaFieldColumnMapTest {
	private Schema schema;

	@Before
	public void setUp() throws Exception {
		schema = new Schema(null,
				Schema.createField(DataType.STRING, "a"),
				Schema.createField(DataType.STRING, "b"),
				Schema.createField(DataType.STRING, "c"),
				Schema.createField(DataType.STRING, "d"));
	}
	
	@Test
	public void justFirstColumn() throws Exception {
		SchemaFieldColumnMapper mapper = new SchemaFieldColumnMapper(schema, "a");
		
		Assert.assertEquals(0, mapper.map(0));
		Assert.assertEquals("x,null,null,null", mapper.mapCSV(new String [] {"x"}));
	}

	@Test
	public void justMiddleColumn() throws Exception {
		SchemaFieldColumnMapper mapper = new SchemaFieldColumnMapper(schema, new String [] {"c"});
		
		Assert.assertEquals(2, mapper.map(0));
		Assert.assertEquals("null,null,x,null", mapper.mapCSV("x"));
	}

	@Test
	public void justLastColumn() throws Exception {
		SchemaFieldColumnMapper mapper = new SchemaFieldColumnMapper(schema, new String [] {"d"});
		
		Assert.assertEquals(3, mapper.map(0));
		Assert.assertEquals("null,null,null,x", mapper.mapCSV("x"));
	}

	@Test
	public void mapAllInOrder() throws Exception {
		SchemaFieldColumnMapper mapper = new SchemaFieldColumnMapper(schema, new String [] {"a", "b", "c", "d"});
		
		Assert.assertEquals("q,w,e,r", mapper.mapCSV(new String [] {"q","w","e","r"}));
	}

	@Test
	public void mapAllOutOfOrder() throws Exception {
		SchemaFieldColumnMapper mapper = new SchemaFieldColumnMapper(schema, new String [] {"b", "d", "c", "a"});
		
		Assert.assertEquals("w,x,y,z", mapper.mapCSV("x","z","y", "w"));
	}
	
	@Test
	public void mapSomeInOrder() throws Exception {
		SchemaFieldColumnMapper mapper = new SchemaFieldColumnMapper(schema, new String [] {"b","c"});
		
		Assert.assertEquals("null,x,y,null", mapper.mapCSV("x","y"));
	}

	@Test
	public void mapSomeOutOfOrder() throws Exception {
		SchemaFieldColumnMapper mapper = new SchemaFieldColumnMapper(schema, new String [] {"d","b"});
		
		Assert.assertEquals("null,y,null,x", mapper.mapCSV("x","y"));
	}
}
