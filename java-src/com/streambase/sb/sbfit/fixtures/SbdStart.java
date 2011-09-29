/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streambase.sb.sbfit.common.util.LogOutputCapture;

import fit.Parse;
import fitnesse.fixtures.TableFixture;

public class SbdStart extends TableFixture {
    private static final Logger logger = LoggerFactory.getLogger(SbdStart.class);

    private SbWithFixture with = null;
    private Map<String, String> params = new HashMap<String, String>();
    private String containerName;
    private Integer port;
    private Integer hb_port;
    private Integer peer_hb_port;

    public SbdStart() {
        with = new SbWithFixture(this, SbFixtureType.SbdStart);
    }

    public void doTable(Parse rows) {
        if (args.length < 2 || args.length > 6) {
            logger.info("SbdStart requires at least two arguments, an optional container name and port and may have a value for each parameter");
            IllegalArgumentException exception = new IllegalArgumentException(
                    "Usage: alias app( container_name( port( hb_port peer_hb_port)?)?)?");
            exception(rows, exception);
            throw new RuntimeException(exception);
        }

        try {
            with.start();

            int limit = args.length;

            containerName = "default";
            port = null;
            hb_port = 5000;
            peer_hb_port = 5001;

            if (limit > 2)
                containerName = args[2];
            if (limit > 3)
                port = new Integer(args[3]);
            if (limit > 5) {
                hb_port = new Integer(args[4]);
                peer_hb_port = new Integer(args[5]);
            }

            if (rows.parts.more != null)
                super.doTable(rows);

            LogOutputCapture.getCapturer().captureOutput();
            with.startSbd(args[0], args[1], containerName, params, port,
                    hb_port, peer_hb_port);
        } catch (NumberFormatException e) {
            logger.info("NumberFormat", e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.info("Runtime exception", e);
            throw new RuntimeException(e);
        } finally {
            with.stop();
        }
    }

    @Override
    protected void doStaticTable(int rows) {
        for (int i = 0; i < rows; i++) {
            params.put(getText(i, 0), getText(i, 1));
        }
    }
}
