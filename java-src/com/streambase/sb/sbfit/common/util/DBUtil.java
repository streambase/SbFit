package com.streambase.sb.sbfit.common.util;

import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.classloader.JarClassLoader;
import com.streambase.sb.jdbc.DataSourceInfo;
import com.streambase.sb.jdbc.DriverFactory;
import com.streambase.sb.sbd.ConfTag;
import com.streambase.sb.sbd.SBDConf;
import com.streambase.sb.util.Msg;
import com.streambase.sb.util.Util;
import com.streambase.sb.util.Xml;

public class DBUtil {
	public ClassLoader mainClassLoader;
	public ClassLoader jarClassLoader;

	/**
     * This is a rip off of the TypecheckEnv internal version
     */
    public class DataSourceInfoImpl implements DataSourceInfo {
        private String uri;
        private String dataSourceName;
        private Properties props;
        private String driverClass;
        boolean registered = false;
        boolean vertica = false;
        private Driver wrapperDriver;
        private boolean quoteStrings = true;
        private int storedProcReturnType;
        private int timeout;
        private int queryTimeoutMs;
        private long batchTimeout;
        private Integer fetchSize;
        private int batchSize;
        private ArrayList<String> retrySQLStates = null;
        private Map zeroOverrides = null;
        private long idleTimeout = 0;

        private ArrayList<Pattern> reconnectRegexp;
        private ArrayList<Pattern> dontReconnectRegexp;
        private int numReconnectAttempts;
        private int numTypecheckReconnectAttempts;
        private long reconnectSleep;
        private boolean shareConnections;
        private Connection sharedConnection;
        
        DataSourceInfoImpl(String ds, String u, Properties p,
                String dc, int to, int queryTimeoutMs, Integer fs,
                int bs, long batchTO,
                ArrayList<String> rss,
                boolean bvertica, boolean shareConn, boolean quoteStr,
                int storedProcReturnType,
                Map zeros, ArrayList<String> reconRegexp, ArrayList<String> noReconnectRegexp,
                int numReconnect, int numTypecheckReconnect, long reconSleep, int idleTimeout)
                    throws StreamBaseException {
            dataSourceName = ds;
            uri = u;
            props = p;
            driverClass = dc;
            vertica = bvertica;
            shareConnections = shareConn;
            quoteStrings = quoteStr;
            this.storedProcReturnType = storedProcReturnType;
            this.idleTimeout = idleTimeout;
            zeroOverrides = zeros;

            timeout = to;
            this.queryTimeoutMs = queryTimeoutMs;
            fetchSize = fs;
            batchSize = bs;
            batchTimeout = batchTO;
            retrySQLStates = rss;

            numReconnectAttempts = numReconnect;
            numTypecheckReconnectAttempts = numTypecheckReconnect;
            reconnectSleep = reconSleep;
            reconnectRegexp = new ArrayList<Pattern>();
            dontReconnectRegexp = new ArrayList<Pattern>();

            for(String pat : noReconnectRegexp) {
                dontReconnectRegexp.add(Pattern.compile(pat));
            }

            for(String pat : reconRegexp) {
                reconnectRegexp.add(Pattern.compile(pat));
            }

            if(uri == null)
                throw new StreamBaseException(MessageFormat.format("data-source {0} missing uri",
                        dataSourceName));
        }

        public String getURI() {return uri; }

        public Properties getProperties() {return props; }

        public String getDriverClass() { return driverClass; }

        public int getTimeout() {return timeout;}

        public long getQueryTimeoutMs() {return queryTimeoutMs;}
        
        public String getDataSourceName() {return dataSourceName;}

        /** Might be null if we should use the driver's default fetch size */
        public Integer getFetchSize() {return fetchSize;}

        public int getBatchSize() {return batchSize;}

        public long getBatchTimeout() {return batchTimeout;}

        public List<String> getRetrySQLStates() {return retrySQLStates;}

        public boolean shareConnections() {return shareConnections;}

        public boolean quoteStrings() {return quoteStrings;}

        public int storedProcReturnType() {return storedProcReturnType;}

        public Map getZeroOverrides() {return zeroOverrides;}

        /**
         * Is this a Chronicle data source?
         */
        public boolean isVertica() {
            return vertica;
        }

        /**
         * Retrieves a JDBC Connection from this TypecheckEnv. If the driver is not
         * yet loaded, such loading takes place now.
         *
         * @return a non-null JDBC Connection for the data source provided
         * @throws StreamBaseException if unable to find any data source of that name, or if the driver could not be loaded for any reason
         */
        public Connection getConnection() throws StreamBaseException {
            return getConnection(numReconnectAttempts());
        }

        /**
         * Retrieves a JDBC Connection from this TypecheckEnv. If the driver is not
         * yet loaded, such loading takes place now.
         * @param numReconnectAttempts number of times to attempt to reconnect if there's an error trying to get a connection
         * @return a non-null JDBC Connection for the data source provided
         * @throws StreamBaseException if unable to find any data source of that name, or if the driver could not be loaded for any reason
         */
        public Connection getConnection(int numReconnectAttempts) throws StreamBaseException {
            if(shareConnections) {
                //shouldn't try to share a closed connection
                if (!isValid(sharedConnection)) {
                    sharedConnection = actuallyGetConnectionWithReconnect(numReconnectAttempts);
                }
                return sharedConnection;
            } else {
                return actuallyGetConnectionWithReconnect(numReconnectAttempts);
            }
        }

        private boolean isValid(Connection conn) {
            if(conn == null) {
                return false;
            }

            try {
                if(conn.isClosed()) {
                    return false;
                }

                // getTransactionIsolation can be more reliable than isClosed.
                // this can cause an exception if the connection isn't live
                // sometimes even an NPE
                conn.getTransactionIsolation(); 

                // While jdk 1.6 adds an isValid() method,
                // not all jdbc implementations implement it
                //conn.isValid(timeout);
                return true;
            } catch (SQLException e) {
                return false;
            } catch (NullPointerException e) {
                return false;
            }
        }

        public void releaseConnection(Connection connection) {
            if(!shareConnections) {
                actuallyReleaseConnection(connection);
            }
        }
        public void actuallyReleaseConnection(Connection connection) {
            if(connection != null) {
                try {
                    //shouldn't try to close a closed connection.
                    if(!connection.isClosed()){
                        connection.close();
                    }
                    //if we closed the shared one, we can't let people keep using it...
                    if(connection == sharedConnection) {
                        sharedConnection = null;
                    }
                } catch (SQLException e) {
                    Msg.debug(e);
                }
            }
        }

        private Connection actuallyGetConnectionWithReconnect(int numReconnectAttempts) throws StreamBaseException {
            if(numReconnectAttempts == 0) {
                return actuallyGetConnection();
            } else {
                StreamBaseException except = null;

                for(int i=0; numReconnectAttempts == -1 ||  i < numReconnectAttempts; ++i) {
                    try {
                        Connection conn = actuallyGetConnection();
                        return conn;
                    } catch (StreamBaseException e) {
                        if(except == null) {
                            except = e;
                        }
                        Msg.error(Msg.format("Exception while trying to establish JDBC connection, attempt {0}: {1}", i, e.getMessage()), e);
                        	try {
								Thread.sleep(reconnectSleep());
							} catch (InterruptedException e1) {
								e1.printStackTrace();
							}
                    }
                }
                throw except;
            }
        }

        private Connection actuallyGetConnection() throws StreamBaseException {
            Msg.debug(Msg.format("data-source: {0} uri: {1} driver class: {2}", dataSourceName, uri, driverClass));

            // Time to register and load ourselves, if not done already
            ensureRegistered();

            // Let's go!
            try {
                Connection c = DriverManager.getConnection(uri, props);

                Msg.debug(Msg.format("Established JDBC Connection to {0}", uri));
                return c;
            } catch (SQLException e) {
                throw new StreamBaseException("SQLException while retrieving JDBC Connection", e);
            }
        }


        private void ensureRegistered() throws StreamBaseException {
            if (!registered) {
                try {
                    DriverManager.getDriver(uri);
                    registered = true;
                } catch (SQLException e) {
                    //do this the hard way
                }
            }

            if (!registered) {
                try {
                    Class c = Class.forName(driverClass, true, getClassLoader());
                    Driver d = (Driver) c.newInstance();
                    wrapperDriver = DriverFactory.makeDriver(d);
                    DriverManager.registerDriver(wrapperDriver);
                    Msg.debug("Data source registered driver " + driverClass + " v " + d.getMajorVersion() + "." + d.getMinorVersion() + " (instance " + wrapperDriver.hashCode() + ")");
                } catch (Exception e) {
                    throw new StreamBaseException("Unable to load JDBC Driver class " + driverClass, e);
                }
                registered = true;
            }
        }

        /**
         * Safely unregister this jdbc connection driver (if previously registered properly)
         * from java.sql.DriverManager
         */
        public void deregister() {
            if (registered) {
                try {
                    DriverManager.deregisterDriver(wrapperDriver);
                    Msg.debug("Data source deregistered driver " + driverClass + "(instance " + wrapperDriver.hashCode() + ")");
                    wrapperDriver = null;
                } catch (Exception e) {
                    Msg.warn("Exception during jdbc driver deregistration: " + driverClass);
                }
            }
        }

        public boolean isReconnectError(String message) {
            Msg.debug("exception message is: {0}", message);

            for(Pattern pat : dontReconnectRegexp) {
                Matcher m = pat.matcher(message);

                Msg.debug("Checking for nomatch pattern {0}", pat);

                if(m.find()) {
                    return false;
                }
            }

            for(Pattern pat : reconnectRegexp) {
                Matcher m = pat.matcher(message);

                Msg.debug("Checking pattern {0}", pat.toString());

                if(m.find())
                    return true;
            }
            return false;
        }

        public int numReconnectAttempts() {
            return numReconnectAttempts;
        }

        public long reconnectSleep() {
            return reconnectSleep;
        }

        public boolean canReconnect() {
            return numReconnectAttempts != -1;
        }

        public String getEncodedText() {
            final String DELIM = "; ";
            return Util.join(DELIM,
                "dataSource=" + getDataSourceName(),
                "batching=" + (getBatchSize() > 0),
                "supportsPrepare=" + !isVertica());
        }

        @Override
        public long getIdleTimeout() {
            return idleTimeout;
        }

        public int numTypecheckReconnectAttempts() {
            return numTypecheckReconnectAttempts;
        }

		@Override
		public int getQueryTimeoutSeconds() {
			// TODO Auto-generated method stub
			return 0;
		}
    }

    public DBUtil() {
        mainClassLoader = new JarClassLoader(getClass().getClassLoader());
        jarClassLoader = new JarClassLoader(mainClassLoader);
	}
    
    private DataSourceInfo makeDataSource(String name, String uri, String driver, Properties props) throws StreamBaseException {
		return new DataSourceInfoImpl(name, uri, props, driver, 
				-1, -1, -1, -1, -1, new ArrayList<String>(), 
				false, true, true, 0, null, 
				new ArrayList<String>(), new ArrayList<String>(), 5, -1, 1000, 60000000);	
    }
	
    private ClassLoader getClassLoader() {
    	return jarClassLoader;
    }
    
	public DataSourceInfo getDataSource(String dsName) throws StreamBaseException {
		File sbconfFile = new File(SBARCache.getSBConfFileName());
		SBDConf conf = new SBDConf(sbconfFile);
		List<ConfTag> tags = conf.getAllModuleTags("data-sources",Xml.NOT_REQUIRED);

		if(tags == null || tags.size() == 0) {
			throw new StreamBaseException(MessageFormat.format("No <data-sources> tags in sbconf file {0}", sbconfFile.getAbsoluteFile()));
		}
		
		List<ConfTag> dataSources = tags.get(0).getAllModuleTags("data-source", Xml.NOT_REQUIRED);
		
		if(dataSources == null || dataSources.size() == 0) {
			throw new StreamBaseException(MessageFormat.format("No <data-source> tags in <data-sources> section in sbconf file {0}", sbconfFile.getAbsoluteFile()));			
		}
		
		for(ConfTag ds : dataSources) {
			String name = ds.attribute("name", Xml.REQUIRED);

			if(name != null && name.equals(dsName)) {
				List<ConfTag> uriTag = ds.getAllModuleTags("uri", Xml.REQUIRED);
				String uri = uriTag.get(0).attribute("value", Xml.REQUIRED);
				List<ConfTag> driverTag = ds.getAllModuleTags("driver", Xml.REQUIRED);
				String driver = driverTag.get(0).attribute("value", Xml.REQUIRED);
				String user = ds.attribute("user", Xml.NOT_REQUIRED);
				String pass = ds.attribute("password", Xml.NOT_REQUIRED);

				//
				// there's a bunch of jdbc attributes that we're ignoring because we don't use them
				//
				List<ConfTag> params = ds.getAllModuleTags("param", Xml.NOT_REQUIRED);
				Properties props = new Properties();
				
				for(ConfTag param : params) {
					String propName = param.attribute("name", Xml.REQUIRED);
					String propVal = param.attribute("value", Xml.REQUIRED);
					
					props.put(propName, propVal);
				}
				
				return makeDataSource(name, uri, driver, props);
			}
		}
		
		throw new StreamBaseException(MessageFormat.format("Data source {0} was not found", dsName));
	}

	
	public static void main(String [] args) {
		DBUtil util = new DBUtil();
		
		try {
			util.getDataSource("myDB");
		} catch (StreamBaseException e) {
			e.printStackTrace();
		}
	}

}