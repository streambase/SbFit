/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.sbfit.common.SbConversation;

import fitlibrary.DoFixture;

public class DefineVariable extends DoFixture {
	public void defineVarInAs(String varName, String sbdAlias, String expression) throws StreamBaseException
	{
		SbConversation conversation = SbClientFactory.getByAlias(sbdAlias);
		
		conversation.defineVariable(varName,expression);
	}
	public void defineVarInUsingVarIn(String varName1, String sbdAlias1, String varName2, String sbdAlias2) throws StreamBaseException
	{
		SbConversation conversation1 = SbClientFactory.getByAlias(sbdAlias1);
		SbConversation conversation2 = SbClientFactory.getByAlias(sbdAlias2);
		
		conversation1.defineVariable(varName1,conversation2.getVariableValue(varName2));
	}
}
