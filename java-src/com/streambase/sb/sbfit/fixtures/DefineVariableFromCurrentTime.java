/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.sbfit.common.SbConversation;

import fitlibrary.DoFixture;

public class DefineVariableFromCurrentTime extends DoFixture {
	static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
	static final SimpleDateFormat hFormat = new SimpleDateFormat("HH");
	static final SimpleDateFormat mFormat = new SimpleDateFormat("mm");
	static final SimpleDateFormat sFormat = new SimpleDateFormat("ss");
	static final SimpleDateFormat dFormat = new SimpleDateFormat("dd");
	static final SimpleDateFormat mnFormat = new SimpleDateFormat("MM");
	static final SimpleDateFormat yFormat = new SimpleDateFormat("yyyy");
	public String defineVarInUsingNow(String varName, String sbdAlias) throws StreamBaseException
	{
		SbConversation conversation = SbClientFactory.getByAlias(sbdAlias);
		
		String value = format.format(new Date(System.currentTimeMillis()));
		conversation.defineVariable(varName,value);
		return value;
	}
	public String defineVarInUsingNowOffset(String varName, String sbdAlias, long offset) throws StreamBaseException
	{
		SbConversation conversation = SbClientFactory.getByAlias(sbdAlias);
		
		String value = format.format(new Date(System.currentTimeMillis()+offset));
		conversation.defineVariable(varName,value);
		return value;
	}
	public String defineVarInAsHoursOfVarFrom(String varName1, String sbdAlias1, String varName2, String sbdAlias2) throws StreamBaseException, ParseException
	{
		SbConversation conversation1 = SbClientFactory.getByAlias(sbdAlias1);
		SbConversation conversation2 = SbClientFactory.getByAlias(sbdAlias2);
		
		Date d = format.parse(conversation2.getVariableValue(varName2));
		
		String value = hFormat.format(d);
		conversation1.defineVariable(varName1,value);
		return value;
	}
	public String defineVarInAsMinutesOfVarFrom(String varName1, String sbdAlias1, String varName2, String sbdAlias2) throws StreamBaseException, ParseException
	{
		SbConversation conversation1 = SbClientFactory.getByAlias(sbdAlias1);
		SbConversation conversation2 = SbClientFactory.getByAlias(sbdAlias2);
		
		Date d = format.parse(conversation2.getVariableValue(varName2));
		
		String value = mFormat.format(d);
		conversation1.defineVariable(varName1,value);
		return value;
	}
	public String defineVarInAsSecondsOfVarFrom(String varName1, String sbdAlias1, String varName2, String sbdAlias2) throws StreamBaseException, ParseException
	{
		SbConversation conversation1 = SbClientFactory.getByAlias(sbdAlias1);
		SbConversation conversation2 = SbClientFactory.getByAlias(sbdAlias2);
		
		Date d = format.parse(conversation2.getVariableValue(varName2));
		
		String value = sFormat.format(d);
		conversation1.defineVariable(varName1,value);
		return value;
	}
	public String defineVarInAsDaysOfVarFrom(String varName1, String sbdAlias1, String varName2, String sbdAlias2) throws StreamBaseException, ParseException
	{
		SbConversation conversation1 = SbClientFactory.getByAlias(sbdAlias1);
		SbConversation conversation2 = SbClientFactory.getByAlias(sbdAlias2);
		
		Date d = format.parse(conversation2.getVariableValue(varName2));
		
		String value = dFormat.format(d);
		conversation1.defineVariable(varName1,value);
		return value;
	}
	public String defineVarInAsMonthsOfVarFrom(String varName1, String sbdAlias1, String varName2, String sbdAlias2) throws StreamBaseException, ParseException
	{
		SbConversation conversation1 = SbClientFactory.getByAlias(sbdAlias1);
		SbConversation conversation2 = SbClientFactory.getByAlias(sbdAlias2);
		
		Date d = format.parse(conversation2.getVariableValue(varName2));
		
		String value = mnFormat.format(d);
		conversation1.defineVariable(varName1,value);
		return value;
	}
	public String defineVarInAsYearsOfVarFrom(String varName1, String sbdAlias1, String varName2, String sbdAlias2) throws StreamBaseException, ParseException
	{
		SbConversation conversation1 = SbClientFactory.getByAlias(sbdAlias1);
		SbConversation conversation2 = SbClientFactory.getByAlias(sbdAlias2);
		
		Date d = format.parse(conversation2.getVariableValue(varName2));
		
		String value = yFormat.format(d);
		conversation1.defineVariable(varName1,value);
		return value;
	}
}
