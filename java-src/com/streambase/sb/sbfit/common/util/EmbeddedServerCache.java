package com.streambase.sb.sbfit.common.util;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.sbfit.fixtures.SbFixtureReporter;
import com.streambase.sb.sbfit.fixtures.SbFixtureType;
import com.streambase.sb.unittest.SBServerManager;
import com.streambase.sb.unittest.ServerManagerFactory;

public class EmbeddedServerCache {
	private static final Logger logger = LoggerFactory.getLogger(EmbeddedServerCache.class);
	private static EmbeddedServerCache singleton = null;
	private SBARCache sbarCache;
	private Map<String,SBServerManager> instances = new HashMap<String, SBServerManager>();
	
	static public EmbeddedServerCache getCache() throws StreamBaseException {
		if ( singleton == null )
			singleton = new EmbeddedServerCache();
		
		return singleton;
	}

	private EmbeddedServerCache() throws StreamBaseException {
		
		sbarCache = SBARCache.getCache();
	}
	
	public SBServerManager getNewEmbeddedServer(String containerName, Map<String, String> params, Integer port, Integer hbPort, Integer peerHbPort, String app) throws StreamBaseException, IOException, InterruptedException {
		File sbarFile = sbarCache.getSBARFile(containerName, app, params, port, hbPort, peerHbPort);
		SBServerManager sbd;
		
		//
		// This instances thing can't actually work because fitnesse spawns a new jvm, runs the test, then kills the jvm
		//
		if (instances.containsKey(sbarFile.getCanonicalPath())) {
			sbd = instances.get(sbarFile.getCanonicalPath());
			logger.info("Reusing {} on port {}", app, port);
		} else {
			try {
			    SbFixtureReporter.reporter.start(SbFixtureType.SbdStart + " - getting sbar");
			    sbarFile = sbarCache.getSBAR(containerName, app, params, port, hbPort, peerHbPort);
			} finally {
			    SbFixtureReporter.reporter.stop(SbFixtureType.SbdStart + " - getting sbar");
			}
			
			SBARCache.setSBConfProperty();
			
			String processUniquifier = System.getenv("SB_UNIQUIFIER");
			processUniquifier = (null != processUniquifier)? processUniquifier : "Default";
			sbd = ServerManagerFactory.getEmbeddedServer();
			
			try {
			    SbFixtureReporter.reporter.start(SbFixtureType.SbdStart + " - starting server");
                logger.info("Starting {} on port {} with params: {}",
                        new Object[] { app, port, params });
                sbd.startServer();

                logger.info("loadApp {} into container {}",
                        sbarFile.getAbsolutePath(), containerName);
                sbd.loadApp(sbarFile.getAbsolutePath(), containerName);
                logger.debug("application {} loaded", sbarFile.getAbsolutePath());
			} finally {
			    SbFixtureReporter.reporter.stop(SbFixtureType.SbdStart + " - starting server");
			}

			instances.put(sbarFile.getCanonicalPath(),sbd);
		}
		
        try {
            SbFixtureReporter.reporter.start(SbFixtureType.SbdStart + " - starting containers");
            sbd.startContainers();
            logger.debug("containers have started");
        } finally {
            SbFixtureReporter.reporter.stop(SbFixtureType.SbdStart + " - starting containers");
        }
		
		return sbd;
	}
}
