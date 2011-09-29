package com.streambase.sb.sbfit.common.util;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import com.streambase.sb.Schema;
import com.streambase.sb.StreamBaseException;

/**
 * Map csv columns to schema columns
 * TODO -- make this work with nested tuples
 */
public class SchemaFieldColumnMapper {
	private Schema s;
	private int [] indexMapping;
	private String [] columnNames;
	
	private static class SchemaColumnMappingException extends StreamBaseException {
		private static final long serialVersionUID = -8172326924262931938L;

		public SchemaColumnMappingException(List<String> columnsNotInSchema) {
			super(MessageFormat.format("The following columns are not in the schema: {0}", columnsNotInSchema));
		}
	}
	
	
	public SchemaFieldColumnMapper(Schema s, String... columnNames) throws StreamBaseException {
		this.s = s;
		this.columnNames = columnNames;
		
		map();
	}
	
	/**
	 * @param colNumber the index (zero-based) of the csv column
	 * @return The schema field index
	 */
	public int map(int colNumber) {
		return indexMapping[colNumber];
	}
	
	public String mapCSV(String... columns) throws StreamBaseException {
		if(columns.length != columnNames.length) {
			throw new StreamBaseException(MessageFormat.format("requires {0} columns, but {1} columns were used", columnNames.length, columns.length));
		}
		
		StringBuilder row = new StringBuilder();
		String [] mappedColumns = new String[s.getFieldCount()];
		
		for(int i=0; i < columns.length; ++i) {
			mappedColumns[indexMapping[i]] = columns[i];
		}
		
		for(String col : mappedColumns) {
			if(row.length() != 0)
				row.append(",");
			row.append(col);
		}
		
		return row.toString();
	}
	
	private void map() throws SchemaColumnMappingException {
		int index = 0;
		List<String> notFound = new ArrayList<String>();
		
		indexMapping = new int[columnNames.length];

		for(String colName : columnNames) {
			int fieldIndex = s.getFieldIndex(colName);
			
			if(fieldIndex < 0) {
				notFound.add(colName);
			} else {
				indexMapping[index++] = fieldIndex;
			}
		}
		
		if(notFound.size() > 0) {
			throw new SchemaColumnMappingException(notFound);
		}
	}

}
