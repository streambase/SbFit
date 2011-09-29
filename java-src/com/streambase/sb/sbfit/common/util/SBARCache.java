package com.streambase.sb.sbfit.common.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.util.Util;

public class SBARCache {
	private static final Logger logger = LoggerFactory.getLogger(SBARCache.class);
	
	private final static String SBARGEN_COMMAND = "sbargen";
	private final static String SBCONF_PROP_NAME = "STREAMBASE_CONFIG";
	/** The system property that sbunit uses for the sbconf file name */
	private final static String SBUNIT_SBCONF_PROP = "streambase.unit-test.server-conf";
	/** PID set by calling process */
	private final static String UNIQUIFIER_PROP_NAME = "SB_UNIQUIFIER";
		
	static private SBARCache singleton = null;
			
	static public SBARCache getCache() throws StreamBaseException
	{
		if ( singleton == null )
			singleton = new SBARCache();
		
		return singleton;
	}
	
	private SBARCache() throws StreamBaseException {
		
	}
	
	public File getSBAR(String containerName, String app, Map<String, String> params, Integer port, Integer hbPort, Integer peerHbPort) throws IOException, StreamBaseException {
		File file = getSBARFile(containerName, app, params, port, hbPort, peerHbPort);
		
		logger.info("Hashed sbarName: {}", file.getCanonicalFile());
		long start = System.currentTimeMillis();
		logger.info("Generating: {}", file.getCanonicalFile());
		generateSBAR(containerName, app, params, file);
		logger.info("Actual name: {} [took {}ms]", file.getCanonicalFile(), System.currentTimeMillis()-start);
		return file;
	}
	
	private void generateSBAR(String containerName, String app,	Map<String, String> params, File sbarFile) throws StreamBaseException {
		try {
			logger.info("Current workding dir is: {}", new File(".").getAbsolutePath());
			logger.info("app file is: {}", app);
			
			execSBARGen(Arrays.asList(new String[] {SBARGEN_COMMAND, "--version"}));

			String appFileName = app;
			ArrayList<String> commands = new ArrayList<String>();
			
			commands.add(SBARGEN_COMMAND);
			commands.add("--if-modified");

			for(Map.Entry<String, String> param : params.entrySet()) {
				commands.add(MessageFormat.format("-P{0}={1}", param.getKey(), param.getValue()));
			}

			String [] conf = getConfigFileArgs();
			
			if(conf != null) {
				for(String c : conf) {
					commands.add(c);
				}
			}

			commands.add(appFileName);
			commands.add(sbarFile.getAbsolutePath());

			execSBARGen(commands);
			
		} catch (Exception e) {
			throw new StreamBaseException(e);
		}
	}
	
	private class DrainThread extends Thread {
		private InputStreamReader in;
		private StringBuilder text = new StringBuilder();

		DrainThread(InputStream is) {
			in = new InputStreamReader(is);
			// don't hold up shutting down the jvm on this
			setDaemon(true);
		}

		public void run() {
			int c;
			try {
				while ((c = in.read()) >= 0) {
					text.append((char)c);
				}
			} catch (IOException e) {
				logger.warn("Error Draining Stream", e);
			}
		}

	}
	
	private void execSBARGen(List<String> commands) throws IOException, InterruptedException, StreamBaseException {
		logger.info("sbargen command: {}", Util.join(" ", commands));
		Process proc = Runtime.getRuntime().exec(commands.toArray(new String [commands.size()]));	
		DrainThread stdout = new DrainThread(proc.getInputStream());
		DrainThread stderr = new DrainThread(proc.getErrorStream());
		stdout.start();
		stderr.start();
		proc.waitFor();
		stdout.join();
		stderr.join();
		logger.info("sbargen stdout:\n{}", stdout.text);
		logger.info("sbargen stderr:\n{}", stderr.text);
		if(proc.exitValue() != 0)
			throw new StreamBaseException(MessageFormat.format("sbargen process returned exit status {0}. Look at sbfit log file for details", proc.exitValue()));
	}

	/** @returns "-f <config-file>" if the config file env var is set, or empty string */ 
	private String [] getConfigFileArgs() {
		String configFileName = System.getenv(SBCONF_PROP_NAME);
		return new String [] {"-f", configFileName};
	}
	
	public File getSBARFile(String containerName, String app, Map<String, String> params, Integer port, Integer hbPort, Integer peerHbPort) {
		String sbarName = getSBARName(containerName, app, params, port);
		String processUniquifier = System.getenv(UNIQUIFIER_PROP_NAME);		
		String fileName = "build/sbars/"+processUniquifier + "-"+ 
				sbarName.substring(0, Math.min(7, sbarName.length()))+"-"+
				sbarName.hashCode()+".sbar"; 

		return new File(fileName);
	}

	
	/**
	 * Create a unique filename that includes the containerName, the app, the port, and all the parameters. This
	 * allows compiling the sbapp with different parameters without overwriting the sbapp file name. 
	 */
	public static String getSBARName(String containerName, String app, Map<String, String> params, Integer port) 
	{
		StringBuilder nameBuilder = new StringBuilder();
		nameBuilder.append(containerName).append("-").append(port).append("-");

		for (Entry<String, String> e : params.entrySet())
			nameBuilder.append(e.getKey()).append("-").append(e.getValue()).append("-");
		nameBuilder.append(app).append(".sbar");
		return nameBuilder.toString();
	}
	
	/**
	 * If the sbunit property has been set, use it, otherwise if we have an sbconf env var, use that.
	 */
	public static void setSBConfProperty() {
		String sbunitConf = System.getProperty(SBUNIT_SBCONF_PROP);
		
		if(sbunitConf == null) {
			String configFileName = System.getenv(SBCONF_PROP_NAME);
			
			if(configFileName != null) {
				System.setProperty(SBUNIT_SBCONF_PROP, configFileName);
			}
		}
	}
}
