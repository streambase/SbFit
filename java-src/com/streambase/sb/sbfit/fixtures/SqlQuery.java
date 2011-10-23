package com.streambase.sb.sbfit.fixtures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.jdbc.DataSourceInfo;
import com.streambase.sb.sbfit.common.util.DBUtil;
import com.streambase.sb.util.Msg;
import com.streambase.sb.util.Util;

import fit.ColumnFixture;
import fit.Fixture;
import fit.Parse;

/**
 * Perform a SQL select and test the return values.
 */
public class SqlQuery extends ColumnFixture {
    private static final Logger logger = LoggerFactory.getLogger(SqlQuery.class);

    private DBUtil dbUtil = new DBUtil();
	private DataSourceInfo dataSource;

	private Statement statement;

	private Connection con;
	
    public SqlQuery() {
	}
	
	@Override
	public void doRows(Parse rows) {
		if(args.length != 2) {
			exception(rows.parts, new Exception("Usage: !|SqlQuery|data-source-name|sql query|"));
			return;
		}
		
		String dataSourceName = args[0];
		String sqlQueryText = args[1];
		ResultSet result = null;
		
		logger.info("SqlQuery from data-source {}, sql query {}", dataSourceName, sqlQueryText);

        try {
			List<String> sqlCommands = new ArrayList<String>();
			String [] rowNames = findTableHeaders(rows.parts);

			dataSource = dbUtil.getDataSource(dataSourceName);

			Parse row = rows;

			while((row=row.more) != null) {
				Parse column = row.parts;

				do {
					logger.info("sql: {}", column.text());
					sqlCommands.add(column.text());
				} while((column = column.more) != null);
			}

			result = executeSql(dataSource, sqlQueryText);
			
			logger.info("DB query returned");

			compareResultToTable(rows, result, rowNames);
		} catch (Exception e) {
			logger.error("SqlScript", e);
			exception(rows, e);
		}
		finally {
			if(statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
				}
				statement = null;
			}
			if(con != null) {
				dataSource.releaseConnection(con);
				con = null;
			}
		}

	}
	
	public String [] findTableHeaders(Parse headerCells) {
        String [] bindingFieldNames = new String[headerCells.size()];

        for (int i = 0; headerCells != null; i++, headerCells = headerCells.more) {
            String text = headerCells.text();
            String name = text;

            bindingFieldNames[i] = name;
            logger.debug("field: {}", name);
        }
        return bindingFieldNames;
	}

	private void compareResultToTable(Parse row, ResultSet result, String [] fieldNames) throws SQLException {
        boolean more = true;
        Map<String, Integer> mapping = findColumnNameMapping(result);
        Parse oldRow = row;
        
        more = result.first();
        
        while((row=row.more) != null) {
            Parse cell = row.parts;
            
            for(String columnName : fieldNames) {
        		String expected = cell.text();
        		String found = null;
        		boolean matched = false;
        		
        		logger.debug("looking for column {}", columnName);

        		if(!more) {
        			// the result set returned less rows than we were expecting
        			wrong(cell);
        		} else {
        			if(isFloatingPoint(mapping.get(columnName))) {
        				double actual = result.getDouble(columnName);
        				double exp = Double.parseDouble(expected);

        				matched = Util.compareDoubles(exp, actual);
        				found = Double.toString(actual);
        				logger.debug(MessageFormat.format("expected double: {0}, actual {1} matched {2}", exp, actual, matched));
        			} else {
        				String actual = result.getString(columnName); 

        				matched = actual.equals(expected);
        				found = actual;
        				logger.debug(MessageFormat.format("expected string: {0}, actual {1} matched {2}", expected, actual, matched));
        			}

        			if(matched) {
        				right(cell);
        			} else {
        				wrong(cell, found);
        			}
        		}

        		cell = cell.more;
            }
            
            oldRow = row;
            if(more) {
            	more = result.next();
            }
        }
        
        if(more) {
        	addUnexpectedRows(oldRow, result, fieldNames);
        }
	}
	
    private void addUnexpectedRows(Parse priorRow, ResultSet result, String [] columnNames) throws SQLException {
        do {
        	addUnexpectedRow(priorRow.last(), result, columnNames);
        } while(result.next());
    }
    
    private void addUnexpectedRow(Parse priorRow, ResultSet result, String [] columnNames) {
        Parse lastRow = priorRow;
        Parse newRow = new Parse("tr", null, null, null);
        lastRow.more = newRow;
        lastRow = newRow;

        try {
        	Parse cell = new Parse("td", "", null, null);
        	String colName = columnNames[0];

        	cell.addToBody(Fixture.gray("? = " + result.getString(colName)));
        	ignore(cell);
        	newRow.parts = cell;

        	for (int column = 1; column < columnNames.length; column++) {
        		colName = columnNames[column];
        		Parse current = new Parse("td", "", null, null);
        		current.addToBody(Fixture.gray("? = " + result.getString(colName)));
        		ignore(current);
        		cell.more = current;
        		cell = current;
        	}
        } catch (Exception e) {
        	exception(newRow, e);
        } catch (Throwable e) {
        	e.printStackTrace();
        }
    }

	
	private boolean isFloatingPoint(int type) {
		return type == Types.FLOAT || type == Types.DOUBLE;
	}

	/** @return map of column name to sql type */
	private Map<String, Integer> findColumnNameMapping(ResultSet result) throws SQLException {
		ResultSetMetaData meta = result.getMetaData();
		Map<String, Integer> mapping = new HashMap<String, Integer>();
		
		for(int i=0; i < meta.getColumnCount(); ++i) {
			mapping.put(meta.getColumnName(i+1), meta.getColumnType(i+1));
		}
		
		return mapping;
	}

	
    /**
     * 
     * @param dataSource
     * @param sql array of sql commands to execute
     * @return array of booleans where each is true iff sql execution for corresponding statement succeeded
     * @throws StreamBaseException
     */
	public ResultSet executeSql(DataSourceInfo dataSource, String sql) throws StreamBaseException {
		con = null;
        statement = null;

        con = dataSource.getConnection(dataSource.numTypecheckReconnectAttempts());

        try {
        	statement = con.createStatement();
        } catch (SQLException e) {
        	throw new StreamBaseException("Unable to create statement", e);
        }

        return execute(statement, sql.trim());
	}

    public void forciblyReleaseConnection() {
        try {
            Connection con = dataSource.getConnection(dataSource.numTypecheckReconnectAttempts());
            if(con != null) {
                dataSource.actuallyReleaseConnection(con);
            }
        } catch (StreamBaseException e) {
            Msg.debug(e);
        }
    }

    /**
     * Execute a command if it is non-empty. If an exception occurs, wrap it in
     * a streambase exception that includes the command.
     * @throws StreamBaseException If anything goes wrong.
     */
	private ResultSet execute(Statement statement, String command) throws StreamBaseException {
        if("".equals(command))
            return null;
        Msg.debug("sqlscript: " + command);
        try {
        	return statement.executeQuery(command);
		} catch (SQLException e) {
		    throw new StreamBaseException(Msg.format("Exception while executing {0}", Util.quote(command)), e);
		}
	}
}
