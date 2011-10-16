package com.streambase.sb.sbfit.fixtures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.jdbc.DataSourceInfo;
import com.streambase.sb.operator.TypecheckException;
import com.streambase.sb.sbfit.common.util.DBUtil;
import com.streambase.sb.sbfit.common.util.SchemaFieldColumnMapper;
import com.streambase.sb.util.Msg;
import com.streambase.sb.util.Util;

import fit.Binding;
import fit.ColumnFixture;
import fit.Parse;

public class SqlQuery extends ColumnFixture {
    private static final Logger logger = LoggerFactory.getLogger(SqlQuery.class);

    private DBUtil dbUtil = new DBUtil();
	private DataSourceInfo dataSource;
	
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
			DataSourceInfo ds = dbUtil.getDataSource(dataSourceName);
			List<String> sqlCommands = new ArrayList<String>();
			String [] rowNames = findTableHeaders(rows.parts);

			logger.info("header: {}", rows.parts.text());
			Parse row = rows;

			while((row=row.more) != null) {
				Parse column = row.parts;

				do {
					logger.info("sql: {}", column.text());
					sqlCommands.add(column.text());
				} while((column = column.more) != null);
			}

			result = executeSql(ds, sqlQueryText);
			
			logger.info("DB query returned");
			
			compareResultToTable(row, result, rowNames);
		} catch (Exception e) {
			logger.error("SqlScript", e);
			exception(rows, e);
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
        List<String> tupleRows = new ArrayList<String>();
        boolean resultDone = false;
        
        while((row=row.more) != null) {
            Parse cell = row.parts;
            
        	for (int column = 0; column < fieldNames.length; column++, cell = cell.more) {
        		String expected = cell.text();
        		String actual = result.getString(fieldNames[column]); 

        		if(!resultDone && actual.equals(expected)) {
        			right(cell);
        		} else {
        			wrong(cell);
        		}
        		resultDone = result.next();
        	}
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
		Connection con = null;
        Statement statement = null;

		try {
			con = dataSource.getConnection(dataSource.numTypecheckReconnectAttempts());

            try {
		        statement = con.createStatement();
            } catch (SQLException e) {
                throw new StreamBaseException("Unable to create statement", e);
            }

            return execute(statement, sql.trim());
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
