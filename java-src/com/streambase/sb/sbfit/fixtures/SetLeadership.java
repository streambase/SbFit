package com.streambase.sb.sbfit.fixtures;

import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.Constants.LeadershipStatus;
import com.streambase.sb.client.StreamBaseAdminClient;
import com.streambase.sb.sbfit.common.util.ProcessRegistry;
import com.streambase.sb.unittest.SBServerManager;

import fit.ColumnFixture;
import fit.Parse;

public class SetLeadership extends ColumnFixture {
    private static final Logger logger = LoggerFactory.getLogger(SetLeadership.class);

	@Override
	public void doRows(Parse rows) {
		if(args.length != 2) {
			logger.error("SetLeadership fixture requires arguments alias (SbdStart fixture alias) and LEADER|NONLEADER");
			throw new IllegalArgumentException("The SetLeadership fixture requires arguments alias (SbdStart fixture alias) and LEADER|NONLEADER");
		}
		
        SBServerManager sbd = ProcessRegistry.get(args[0]);

        if(sbd == null) {
			throw new IllegalArgumentException(MessageFormat.format("Cannot find SbStart fixture alias {0}", args[0]));
        }
        
        LeadershipStatus newStatus;
		try {
			newStatus = LeadershipStatus.valueOf(args[1].toUpperCase());
		} catch (Exception e1) {
			throw new IllegalArgumentException(MessageFormat.format("illegal leadership status {0}, must be {1} or {2}", 
					args[1], LeadershipStatus.LEADER, LeadershipStatus.NON_LEADER));
		}
        
        logger.info("SetLeadershipStatus: setting {}", newStatus);
        
        StreamBaseAdminClient admin = null;
        
        try {
        	admin = new StreamBaseAdminClient(sbd.getURI());
			
			admin.setLeadershipStatus(newStatus);
        } catch (StreamBaseException e) {
			throw new RuntimeException(e);
		}
        finally {
        	if(admin != null) {
        		admin.close();
        	}
        }
	}

}
