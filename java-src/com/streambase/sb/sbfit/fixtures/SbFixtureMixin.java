/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import com.streambase.sb.StreamBaseException;

import fit.Parse;

public interface SbFixtureMixin {
	public Parse doArgs(Parse rows, String... args) throws StreamBaseException;
	public void initBindings(Parse headerCells) throws StreamBaseException;
	public void wrong(Parse cells);
	public void enqueueRow(Parse row) throws Throwable;
}