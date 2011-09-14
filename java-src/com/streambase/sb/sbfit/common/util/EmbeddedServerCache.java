package com.streambase.sb.sbfit.common.util;

import java.util.*;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.unittest.BaseTestEnvironment;
import com.streambase.sb.unittest.SBTestEnvironment;
import com.streambase.sb.unittest.internal.embedded.EmbeddedServerManager;

public class EmbeddedServerCache {

	static private EmbeddedServerCache singleton = null;
	static public EmbeddedServerCache getCache() {
		if ( singleton == null )
			singleton = new EmbeddedServerCache();
		
		return singleton;
	}

	private HashMap< String, EmbeddedServerManager > cache;
	private SBTestEnvironment env;
	private EmbeddedServerCache() {
		cache = new HashMap< String, EmbeddedServerManager >();
		
		String config = System.getenv( "STREAMBASE_CONFIG" );
		if ( config != null ) {
			env = new SbFixtureTestEnvironment( config );
		} else {
			env = BaseTestEnvironment.DEFAULT_ENVIRONMENT;
		}
	}
	
	public EmbeddedServerManager getNewEmbeddedServer( String app ) throws StreamBaseException, InterruptedException {
		
		EmbeddedServerManager sbd;
		if ( cache.containsKey( app ) ) {
			sbd = cache.get( app );
			DebugLogging.getLogger().debugLog( "Reusing old server for " + app );
		}
		else {
			sbd = new EmbeddedServerManager( env );
			DebugLogging.getLogger().debugLog( "Starting " + app );
			sbd.startServer();
			sbd.loadApp(app);
			cache.put( app, sbd );
		}
		
		sbd.startContainers();
		
		return sbd;
	}
}
