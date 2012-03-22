package com.streambase.sb.sbfit.fixtures;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fit.ColumnFixture;
import fit.Parse;

public class UnorderedDequeue extends ColumnFixture {
    private static final Logger logger = LoggerFactory.getLogger(Dequeue.class);
    private SbWithFixture with = null;

    public UnorderedDequeue() {
        with = new SbWithFixture(this, SbFixtureType.UnorderedDequeue);
    }
    
    public void doRows(Parse rows) {
        try {
            with.start();
            logger.debug("args {}", Arrays.asList(args));
            rows = with.doDequeueArgs(rows, args);
            if (rows == null) {
                return;
            }
            with.initBindings(rows.parts);
            with.unorderedDequeue(rows);
        } catch (Throwable e) {
            logger.error("UnorderedDequeue", e);
            exception(rows.parts, e);
        } finally {
            with.stop();
        }
    }
}
