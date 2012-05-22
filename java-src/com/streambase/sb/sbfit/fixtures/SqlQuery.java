package com.streambase.sb.sbfit.fixtures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.Timestamp;
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
					logger.debug("sql: {}", column.text());
					sqlCommands.add(column.text());
				} while((column = column.more) != null);
			}

			result = executeSql(dataSource, sqlQueryText);
			
			logger.debug("DB query returned");

			compareResultToTable(rows, result, rowNames);
		} catch (Exception e) {
			logger.error("SqlQuery exception", e);
			exception(rows.parts, e);
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

	private void compareResultToTable(Parse row, ResultSet result, String [] fieldNames) throws SQLException, StreamBaseException {
        boolean more = true;
        Parse oldRow = row;
        ResultSetMetaData meta = result.getMetaData();
        
        more = result.next();
        
        while((row=row.more) != null) {
            Parse cell = row.parts;
            int colNumber = 0;
            
            for(String columnName : fieldNames) {
        		String expected = cell.text();
        		
        		logger.debug("looking for column {}", columnName);

        		++colNumber;
        		if(!more) {
        			// the result set returned less rows than we were expecting
        			wrong(cell);
        		} else {
        			// empty cell means match anything
        			if(expected.trim().length() == 0) {
        				right(cell);
        			} else {
        				compareColumn(result, meta, cell, colNumber, columnName, expected);
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
	
	/**
	 * If the column was null, compare and mark the cell right or wrong.
	 * 
	 *  @return true if column was null (so the cell has already been marked).
	 */
	private boolean checkNull(Parse cell, ResultSet result, String expected, String found) throws SQLException {
		boolean matched = false;
		
		expected = expected.trim();
		
		if(result.wasNull()) {	
			matched = "null".equals(expected);			

			if(matched) {
				right(cell);
			} else {
				wrong(cell, "null");
			}
			return true;
		} else if("null".equals(expected)) {
			wrong(cell, found);
			return true;
		}
		return false;
	}
	
	private void compareColumn(ResultSet result, ResultSetMetaData meta, Parse cell, int colNumber, String columnName, String expected) throws SQLException, StreamBaseException {
		String found;
		boolean matched;
		
		int type = meta.getColumnType(colNumber);

		switch(type) {
		  case Types.BIT:
		  case Types.BOOLEAN:
		  {
			  boolean actual = result.getBoolean(columnName);

			  if(checkNull(cell, result, expected, Boolean.toString(actual))) {
				  return;
			  }

			  boolean exp = Boolean.parseBoolean(expected);

			  matched = actual == exp;
			  found = Boolean.toString(actual);
			  logger.debug(MessageFormat.format("expected boolean: {0}, actual {1} matched {2}", exp, actual, matched));
		  }
		  break;

		  case Types.REAL:
		  case Types.FLOAT:
		  case Types.DOUBLE:
		  case Types.DECIMAL:
		  case Types.NUMERIC:
		  {
			  double actual = result.getDouble(columnName);

			  if(checkNull(cell, result, expected, Double.toString(actual))) {
				  return;
			  }

			  double exp = Double.parseDouble(expected);

			  matched = Util.compareDoubles(exp, actual);
			  found = Double.toString(actual);
			  logger.debug(MessageFormat.format("expected double: {0}, actual {1} matched {2}", exp, actual, matched));
		  }
		  break;

		  case Types.TINYINT:
		  case Types.SMALLINT:
		  case Types.INTEGER:
		  case Types.BIGINT:
		  {
			  long actual = result.getLong(columnName);

			  if(checkNull(cell, result, expected, Long.toString(actual))) {
				  return;
			  }

			  long exp = Long.parseLong(expected);

			  matched = actual == exp;
			  found = Long.toString(actual);
			  logger.debug(MessageFormat.format("expected long or int: {0}, actual {1} matched {2}", exp, actual, matched));
		  }
		  break;

		  case Types.DATE:
		  case Types.TIME:
		  case Types.TIMESTAMP:
		  {
			  java.sql.Timestamp dbVal = result.getTimestamp(columnName);

			  if(checkNull(cell, result, expected, dbVal == null ? "null" : dbVal.toString())) {
				  return;
			  }

			  Timestamp actual = new Timestamp(dbVal);
			  Timestamp exp = Timestamp.fromString(expected); 

			  matched = actual.equals(exp);
			  found = actual.toString();
			  logger.debug(MessageFormat.format("expected timestamp: {0}, actual {1} matched {2}}", exp, actual, matched));
		  }
		  break;

		  case Types.CHAR:
		  case Types.VARCHAR:
		  case Types.LONGVARCHAR:
		  default:
		  {
			  String actual = result.getString(columnName); 

			  if(checkNull(cell, result, expected, actual)) {
				  return;
			  }

			  matched = actual.equals(expected);
			  found = actual;
			  logger.debug(MessageFormat.format("expected string: {0}, actual {1} matched {2}", expected, actual, matched));
		  }
		}

		if(matched) {
			right(cell);
		} else {
			wrong(cell, found);
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

    /*
     * Execute a command if it is non-empty. If an exception occurs, wrap it in
     * a StreamBaseException that includes the command.
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
