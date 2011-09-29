/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import fit.ColumnFixture;
import fit.Parse;

public class DefineVariableUsingValueDateCalc extends ColumnFixture {
    private SbWithFixture with = null;

    public DefineVariableUsingValueDateCalc() {
        with = new SbWithFixture(this, SbFixtureType.DefineVariable);
    }

    public void doRows(Parse rows) {
        try {
            with.start();
            rows = with.doArgs(rows, args);

            if (rows == null) {
                return;
            }

            with.defineVariableUsingValuedateCalc(rows);
        } catch (Throwable e) {
            exception(rows.parts, e);
        } finally {
            with.stop();
        }
    }
}
