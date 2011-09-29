/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.sbfit.common.SbConversation;

import fitlibrary.DoFixture;

public class ShowVariable extends DoFixture {
	public String varFrom(String varName, String sbdAlias) throws StreamBaseException
	{
		SbConversation conversation = SbClientFactory.getByAlias(sbdAlias);
		
		return conversation.getVariableValue(varName);
	}
}
