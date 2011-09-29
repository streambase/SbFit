/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import java.util.HashMap;
import java.util.Map;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.sbfit.common.SbConversation;

public class SbClientFactory {
	private static final Map<String,SbConversation> connMap = new HashMap<String, SbConversation>();

	public static SbConversation getByAlias(String alias) throws StreamBaseException {
		if (connMap.containsKey(alias)) {
			return connMap.get(alias);
		}
		synchronized(connMap) {
			connMap.put(alias, new SbConversation(alias));
		}
		return connMap.get(alias);
	}

	public static boolean close(String alias) throws StreamBaseException {
		if (connMap.containsKey(alias)) {
			synchronized(connMap) {
				connMap.remove(alias);
			}
		}
		return true;
	}

	public static SbConversation getClient(String alias) {
		return connMap.get(alias);
	}
}
