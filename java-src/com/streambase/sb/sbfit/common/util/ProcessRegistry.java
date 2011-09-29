/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.common.util;

import java.util.HashMap;
import java.util.Map;

import com.streambase.sb.unittest.SBServerManager;

public class ProcessRegistry {
	private static Map<String, SBServerManager> processMap = new HashMap<String, SBServerManager>();
	
	private ProcessRegistry() { }
	
	public static void register(String alias, SBServerManager bpi) {
		synchronized(processMap) {
			processMap.put(alias, bpi);
		}
	}
	
	public static SBServerManager get(String alias) {
		synchronized(processMap) {
			return processMap.get(alias);
		}
	}
	
	public static SBServerManager remove(String alias) {
		synchronized(processMap) {
			return processMap.remove(alias);
		}
	}
	
	public static boolean contains(String alias) {
		synchronized(processMap) {
			return processMap.containsKey(alias);
		}
	}
}
