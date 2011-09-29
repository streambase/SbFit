package com.streambase.sb.sbfit.fixtures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fit.ColumnFixture;
import fit.Parse;

public class Dequeue extends ColumnFixture {
    private static final Logger logger = LoggerFactory.getLogger(Dequeue.class);

    private SbWithFixture with = null;

    public Dequeue() {
        with = new SbWithFixture(this, SbFixtureType.Dequeue);
    }

    public void doRows(Parse rows) {
        try {
            with.start();
            rows = with.doDequeueArgs(rows, args);
            if (rows == null) {
                return;
            }
            with.initBindings(rows.parts);
            with.newDequeue(rows);
        } catch (Throwable e) {
            logger.error("Dequeue", e);
            exception(rows.parts, e);
        } finally {
            with.stop();
        }

    }
}
