package com.streambase.sb.sbfit.common;
/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */

import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streambase.sb.Schema;
import com.streambase.sb.StreamBaseException;
import com.streambase.sb.Tuple;
import com.streambase.sb.client.StreamBaseAdminClient;
import com.streambase.sb.client.StreamBaseClient;
import com.streambase.sb.sbfit.common.util.ProcessRegistry;
import com.streambase.sb.sbfit.fixtures.Dequeue;
import com.streambase.sb.unittest.Enqueuer;
import com.streambase.sb.unittest.SBServerManager;

public class SbConversation {
    private static final Logger logger = LoggerFactory.getLogger(Dequeue.class);
	private static StreamBaseClient sbc;
	private final String alias;
	private final List<FqTuple> tbe = new LinkedList<FqTuple>();
    private Map<String, String> variableMap = new HashMap<String, String>();
        
	public static class FqTuple {
		public String streamName;
		public Tuple tuple;
		public FqTuple(String streamName, Tuple tuple) {
			this.streamName = streamName;
			this.tuple = tuple;
		}
	}
	
	public static StreamBaseClient getLiveClient() throws StreamBaseException {
		if ( sbc != null )
			return sbc;
		
		String config = System.getenv( "STREAMBASE_URI" );
		if ( config != null && config.startsWith( "sb://" ) ) {
			sbc = new StreamBaseClient( config );
			return sbc;
		}
		
		return null;
	}
	
	public StreamBaseAdminClient getAdminClient() throws StreamBaseException {
		return new StreamBaseAdminClient(getSbd().getURI());
	}
	
	public static boolean isTestMode() throws StreamBaseException {
		return ( getLiveClient() == null );
	}

	public SbConversation(String alias) throws StreamBaseException {
		this.alias = alias;
	}
	
	private SBServerManager getSbd()
	{
		return ProcessRegistry.get(alias);
	}
	
	public String getAlias() {
		return alias;
	}

	public List<FqTuple> getToBeEnqueued() {
		return tbe;
	}
	
	public void shuwdownContainer(String containerName) throws StreamBaseException {
		getSbd().stopContainers();
	}
	
	public void addContainer(String containerName, String file) throws StreamBaseException {   
		getSbd().loadApp(file, containerName);
	}
	
    public void defineVariable(String variableName, String value) {
        if (variableName.equalsIgnoreCase("null")) {
            throw new IllegalArgumentException("value null not allowed");
        }
        if (variableMap.containsKey(variableName)) {
            return;
        }
        variableMap.put(variableName, value == "null" ? null : value);
    }
	
    public void resetVariable(String variableName, String value) {
        if (variableName.equalsIgnoreCase("null")) {
            throw new IllegalArgumentException("value null not allowed");
        }
        variableMap.put(variableName, value);
    }
	
	public void updateVariable(String variableName, String operator, String value) {
        if(variableName.equalsIgnoreCase("null") || operator.equals("null") || value.equalsIgnoreCase("null")){
            throw new IllegalArgumentException("value null not allowed");
        }
        if(!variableMap.containsKey(variableName)){
            throw new IllegalArgumentException("variable not found");
        }
        if(operator.matches("[+-/*]"))//arithmetic calculation
        {
            long lValue = Long.parseLong(value);
            long variableValue = Long.parseLong(variableMap.get(variableName).toString());
            if(operator.equals("+")){
                variableValue = variableValue + lValue;
            }
            else if(operator.equals("-")){
                variableValue = variableValue - lValue;
            }
            variableMap.put(variableName, String.valueOf(variableValue));
        }        
    }
	
	public String getVariableValue(String variableName){
	    if(!variableMap.containsKey(variableName)){
            throw new IllegalArgumentException("variable: "+variableName+" not found");
        }
	    return variableMap.get(variableName);
	}
	
	public Collection<String> getVariableNames(){
	    return variableMap.keySet();
	}

	public void enqueue(String streamName, Tuple tuple) throws StreamBaseException {
		if ( !isTestMode() ) {
			getLiveClient().enqueue( streamName, tuple );
			return;
		}
		
		if(logger.isDebugEnabled())
			logger.debug("enqueueing {}", tuple.toString(true));
		getEnqueuer(streamName).enqueue(tuple);
	}

	public Schema getSchemaForStream(String streamName) throws StreamBaseException {
		if ( !isTestMode() )
			return getLiveClient().getSchemaForStream( streamName );
		
		return getEnqueuer(streamName).getSchema();
	}
	
	private Enqueuer getEnqueuer(String streamname) throws StreamBaseException {
		SBServerManager sbd = getSbd();
		
		if(sbd == null)
			throw new StreamBaseException("Embedded sbd has not been started");
		
		Enqueuer e = sbd.getEnqueuer(streamname);
		
		if(e == null)
			throw new StreamBaseException(MessageFormat.format("Can't find stream named {0}", streamname));
		
		return e;
	}
}
