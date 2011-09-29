package com.streambase.sb.sbfit.fixtures;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.sbfit.common.SbConversation;

import fitlibrary.DoFixture;

public class Assert extends DoFixture {
	static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
	public boolean varFromEquals(String varName, String sbdAlias, String expression) throws Throwable
	{
		SbConversation conversation = SbClientFactory.getByAlias(sbdAlias);
		
		return conversation.getVariableValue(varName).equals(expression);
	}
	public boolean varFromEqualsVarFrom(String varName, String sbdAlias1, String otherVarName, String sbdAlias2) throws Throwable
	{
		SbConversation conversation1 = SbClientFactory.getByAlias(sbdAlias1);
		SbConversation conversation2 = SbClientFactory.getByAlias(sbdAlias2);
		return conversation1.getVariableValue(varName).equals(conversation2.getVariableValue(otherVarName));
	}
	public boolean varFromGreaterThanVarFrom(String varName, String sbdAlias1, String otherVarName, String sbdAlias2) throws Throwable
	{
		double parseDouble1 = getAsDouble(varName, sbdAlias1);
		double parseDouble2 = getAsDouble(otherVarName, sbdAlias2);
		return parseDouble1 > parseDouble2;
		
	}
	public boolean varFromLessThanVarFrom(String varName, String sbdAlias1, String otherVarName, String sbdAlias2) throws Throwable
	{
		double parseDouble1 = getAsDouble(varName, sbdAlias1);
		double parseDouble2 = getAsDouble(otherVarName, sbdAlias2);
		return parseDouble1 < parseDouble2;
		
	}
	public boolean varFromGreaterThanOrEqualsVarFrom(String varName, String sbdAlias1, String otherVarName, String sbdAlias2) throws Throwable
	{
		double parseDouble1 = getAsDouble(varName, sbdAlias1);
		double parseDouble2 = getAsDouble(otherVarName, sbdAlias2);
		return parseDouble1 >= parseDouble2;
		
	}
	public boolean varFromLessThanOrEqualsVarFrom(String varName, String sbdAlias1, String otherVarName, String sbdAlias2) throws Throwable
	{
		double parseDouble1 = getAsDouble(varName, sbdAlias1);
		double parseDouble2 = getAsDouble(otherVarName, sbdAlias2);
		return parseDouble1 <= parseDouble2;
		
	}
	private double getAsDouble(String varName, String sbdAlias)
			throws StreamBaseException, ParseException {
		SbConversation conversation1 = SbClientFactory.getByAlias(sbdAlias);
		String variableValue = conversation1.getVariableValue(varName);
		double parseDouble1 = 0;
		try 
		{
			parseDouble1 = Double.parseDouble(variableValue);
		} catch(NumberFormatException nfe)
		{
			parseDouble1 = format.parse(variableValue).getTime();
		}
		return parseDouble1;
	}
}
