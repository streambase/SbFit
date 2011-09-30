package com.streambase.sb.sbfit.fixtures;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.jdbc.DataSourceInfo;
import com.streambase.sb.sbfit.common.util.DBUtil;
import com.streambase.sb.util.Msg;
import com.streambase.sb.util.Util;
import com.sun.tools.internal.ws.wscompile.Options.Target;

import fit.ColumnFixture;
import fit.Parse;

public class SqlScript extends ColumnFixture {
    private static final Logger logger = LoggerFactory.getLogger(SqlScript.class);

    private DBUtil dbUtil = new DBUtil();
	private DataSourceInfo dataSource;
	
	private class Result {
		boolean success;
		Error error;
		
		Result(boolean s, Error e) {
			success = s;
			error = e;
		}
	}

    public SqlScript() {
	}
	
	@Override
	public void doRows(Parse rows) {
		if(args.length <  1 || args.length > 2) {
			exception(rows.parts, new Exception("Usage: !|SqlScript|data-source-name|[ignore errors: true|false]|"));
			return;
		}
		
		String dataSourceName = args[0];
		boolean ignoreErrors = false;
		List<Result> result = null;
		
		if(args.length == 2) {
			ignoreErrors = Boolean.parseBoolean(args[1]);
		}
		
		logger.info("SqlScript from data-source {}, ignore errors {}", dataSourceName, ignoreErrors);

		try {
			DataSourceInfo ds = dbUtil.getDataSource(dataSourceName);
			List<String> sqlCommands = new ArrayList<String>();

			logger.info("header: {}", rows.parts.text());
			Parse row = rows;

			while((row=row.more) != null) {
				Parse column = row.parts;

				do {
					logger.info("sql: {}", column.text());
					sqlCommands.add(column.text());
				} while((column = column.more) != null);
			}

			result = executeSql(ds, ignoreErrors, (String []) sqlCommands.toArray(new String [0]));

			if(result != null) {
				row = rows;
			    int i=0;

				while((row=row.more) != null) {
					Parse column = row.parts;

					do {
						if(result.get(i).success) {
							right(column);
						} else if(result.get(i).error == null ){
							wrong(column);
						} else {
							exception(column, result.get(i).error);
						}
						++i;
					} while((column = column.more) != null);
				}
			}
		} catch (StreamBaseException e) {
			logger.error("SqlScript", e);
			exception(rows, e);
		}
	}
	
    /**
     * 
     * @param dataSource
     * @param sql array of sql commands to execute
     * @return array of booleans where each is true iff sql execution for corresponding statement succeeded
     * @throws StreamBaseException
     */
	public List<Result> executeSql(DataSourceInfo dataSource, boolean ignoreErrors, String [] sql) throws StreamBaseException {
		Connection con = null;
        Statement statement = null;
        List<Result> result = new ArrayList<SqlScript.Result>();

		try {
			con = dataSource.getConnection(dataSource.numTypecheckReconnectAttempts());

            try {
		        statement = con.createStatement();
            } catch (SQLException e) {
                throw new StreamBaseException("Unable to create statement", e);
            }

            for(int i=0; i < sql.length; ++i) {
            	boolean success = false;
            	Error e = null;
            	
            	try {
					success = execute(statement, sql[i].trim());
				} catch (Throwable ex) {
					e = new Error(ex);
				}
				result.add(new Result(success, e));
				
				if(e != null && !ignoreErrors) {
					throw e;
				}
            }
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
		return result;
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
	private boolean execute(Statement statement, String command) throws StreamBaseException {
        if("".equals(command))
            return false;
        Msg.debug("sqlscript: " + command);
        try {
            statement.execute(command);
		} catch (SQLException e) {
		    throw new StreamBaseException(Msg.format("Exception while executing {0}", Util.quote(command)), e);
		}
		return true;
	}
}
