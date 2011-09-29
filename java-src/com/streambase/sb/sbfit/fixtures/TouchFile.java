package com.streambase.sb.sbfit.fixtures;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fit.Parse;
import fitlibrary.DoFixture;

/**
 * Touch a file -- the unix touch command. It updates the timestamp of an existing file, if the file doesn't exist
 * it creates an empty file.
 */
public class TouchFile extends DoFixture {
    private static final Logger logger = LoggerFactory.getLogger(TouchFile.class);

	@Override
	public void doTable(Parse table) {
		super.doTable(table);
		
		if(args.length != 1) {
			logger.error("No argument to TouchFile");
			throw new IllegalArgumentException("The TouchFile fixture requires an argument that is the filename to touch");
		}
		


		try {
			File f = new File(args[0]);
			logger.info("Touching file {}", f.getAbsolutePath());
			
			if(!f.createNewFile()) {
				if(!f.setLastModified(System.currentTimeMillis())) {
					String errorMsg = MessageFormat.format("Could not change file modification time for file {0}", f.getAbsolutePath());

					logger.error(errorMsg);
					throw new RuntimeException(errorMsg);
				}
			}
		} catch (IOException e) {
			logger.error("Error touching file", e);
			throw new RuntimeException(e);
		}
	}

}
