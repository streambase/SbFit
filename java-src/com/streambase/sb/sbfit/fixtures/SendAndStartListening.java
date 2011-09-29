/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import fit.Parse;
import fit.exception.FitParseException;
import fitlibrary.SequenceFixture;

public class SendAndStartListening extends SequenceFixture {
    private SbWithFixture with = null;

    public SendAndStartListening() {
        with = new SbWithFixture(this, SbFixtureType.Blast);
    }

    public void doTable(Parse rows) {
        try {
            with.start();
            if (args.length != 1) {
                exception(rows, new IllegalArgumentException());
            }

            with.doArgs(rows, args);
        } catch (Throwable e) {
            try {
                exception(new Parse("foo"), e);
            } catch (FitParseException ignoreAndContinue) {
            }
        } finally {
            with.stop();
        }
    }
}
