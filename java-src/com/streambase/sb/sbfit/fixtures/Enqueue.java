/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fit.ColumnFixture;
import fit.Parse;

public class Enqueue extends ColumnFixture {
    private static final Logger logger = LoggerFactory.getLogger(Enqueue.class);

    private SbWithFixture with = null;

    public Enqueue() {
        with = new SbWithFixture(this, SbFixtureType.Enqueue);
    }

    public void doRows(Parse rows) {
        try {
            with.start();
            rows = with.doArgs(rows, args);

            if (rows == null) {
                return;
            }

            with.initBindings(rows.parts);

            with.enqueue(rows);

        } catch (Throwable e) {
            logger.info("Enqueue", e);
            exception(rows.parts, e);
        } finally {
            with.stop();
        }
    }
}
