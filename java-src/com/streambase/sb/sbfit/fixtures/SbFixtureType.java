/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

public enum SbFixtureType {
	SbdStart,
	Sbadmin,
	SbdStop,
	Blast,
	Enqueue,
    Dequeue,
    UnorderedDequeue,
    UnorderedDequeueCSV,
    BlockingDequeue,
    NotInDequeue,
    DefineVariable,
    UpdateVariable,
    BlockingDequeueVariableWithDrain,
    SqlScript,
    SqlQuery,
    Touch,
    Wait,
}
