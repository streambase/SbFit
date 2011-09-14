package com.streambase.sb.sbfit.common;
/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */

 

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.streambase.sb.sbfit.common.util.ProcessRegistry;

import com.streambase.sb.Schema;
import com.streambase.sb.StreamBaseException;
import com.streambase.sb.Tuple;
import com.streambase.sb.containers.ContainerManager;
import com.streambase.sb.unittest.internal.embedded.EmbeddedServerManager;

public class SbConversation {
	public static class FqTuple {
		public String streamName;
		public Tuple tuple;
		public FqTuple(String streamName, Tuple tuple) {
			this.streamName = streamName;
			this.tuple = tuple;
		}
	}

	private final String alias;
	private final List<FqTuple> tbe = new LinkedList<FqTuple>();
    private static Map<String, String> variableMap = new HashMap<String, String>();
    
    private final EmbeddedServerManager sbd;
    
	public SbConversation(String alias) throws StreamBaseException {
		this.alias = alias;
		sbd = ProcessRegistry.get(alias);
	}
	
	public String getAlias() {
		return alias;
	}

	public List<FqTuple> getToBeEnqueued() {
		return tbe;
	}
	
	public void shuwdownContainer(String containerName) throws StreamBaseException {
        ContainerManager containerManager = sbd.getContainerManager();
        containerManager.getContainer(containerName).stop();
	}
	
	public void addContainer(String containerName, String file) throws StreamBaseException {   
		sbd.loadApp(file, containerName);
	}
	
	public void defineVariable(String variableName, String value) {
	    if(variableName.equalsIgnoreCase("null") || value.equalsIgnoreCase("null")){
	        throw new IllegalArgumentException("value null not allowed");
	    }
	    if(variableMap.containsKey(variableName)){
	        return;
	    }
//	    long lValue = Long.parseLong(value);
	    variableMap.put(variableName, value);
	    System.out.println("Defined variable:"+variableName+":"+value);
	}
	
	public void resetVariable(String variableName, String/*long*/ value){
        if(variableName.equalsIgnoreCase("null")){
            throw new IllegalArgumentException("value null not allowed");
        }
        if(!variableMap.containsKey(variableName)){
            throw new IllegalArgumentException("variable not found");
        }
        variableMap.put(variableName, value);
        System.out.println("Reset variable:"+variableName+":"+value);
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

	public void enqueue(String streamName, Tuple tuple) throws StreamBaseException {
		sbd.getEnqueuer(streamName).enqueue(tuple);
	}

	public Schema getSchemaForStream(String streamName) throws StreamBaseException {
		return sbd.getEnqueuer(streamName).getSchema();
	}
}
