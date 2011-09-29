/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streambase.sb.sbfit.common.util.LogOutputCapture;

import fit.Parse;
import fitlibrary.SequenceFixture;

public class SbdStop extends SequenceFixture {
    private static final Logger logger = LoggerFactory.getLogger(SbdStop.class);

    private SbWithFixture with = null;

    public SbdStop() {
        with = new SbWithFixture(this, SbFixtureType.SbdStart);
    }

    public void doTable(Parse rows) {
        if (args.length != 1) {
            logger.info("SbdStop requires one argument");
            exception(rows,
                    new IllegalArgumentException("Usage: arg0: <alias>"));
        }

        try {
            with.start();
            logger.info("Running sbd stop {}", args[0]);
            with.stopSbd(args[0]);
            LogOutputCapture.getCapturer().reset();
        } catch (Exception e) {
            logger.info("Runtime exception stopping", e);
            throw new RuntimeException(e);
        } finally {
            with.stop();
        }
    }
}
