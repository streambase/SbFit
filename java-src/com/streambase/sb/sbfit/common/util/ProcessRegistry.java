/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.common.util;

import java.util.HashMap;
import java.util.Map;

import com.streambase.sb.unittest.internal.embedded.EmbeddedServerManager;

public class ProcessRegistry {
	private static Map<String, EmbeddedServerManager> processMap = new HashMap<String, EmbeddedServerManager>();
	
	private ProcessRegistry() { }
	
	public static void register(String alias, EmbeddedServerManager bpi) {
		synchronized(processMap) {
			processMap.put(alias, bpi);
		}
	}
	
	public static EmbeddedServerManager get(String alias) {
		synchronized(processMap) {
			return processMap.get(alias);
		}
	}
	
	public static EmbeddedServerManager remove(String alias) {
		synchronized(processMap) {
			return processMap.remove(alias);
		}
	}
}
