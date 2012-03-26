package com.streambase.sb.sbfit.fixtures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.client.StreamBaseAdminClient;
import com.streambase.sb.sbfit.common.util.ProcessRegistry;
import com.streambase.sb.unittest.SBServerManager;

import fit.ColumnFixture;
import fit.Parse;

public class GetLeadership extends ColumnFixture {
    private static final Logger logger = LoggerFactory.getLogger(GetLeadership.class);

	@Override
	public void doRows(Parse rows) {
		if(args.length != 1) {
			logger.error("GetLeadership fixture has 1 required alias argument");
			throw new IllegalArgumentException("The GetLeadership fixture requires an argument that is the alias of the server used in the SbdStart fixture");
		}
		
        SBServerManager sbd = ProcessRegistry.get(args[0]);
		StreamBaseAdminClient admin = null;

        try {
			admin = new StreamBaseAdminClient(sbd.getURI());
			
			String leadership = admin.getLeadershipStatus().toString();
			
			logger.info("GetLeadership: {}", leadership);
			
			if(rows.more != null) {
				Parse column = rows.parts;
				
				logger.info("GetLeadership: expected {}", column.text());
				
				if(leadership.equalsIgnoreCase(column.text())) {
					right(column);
				} else {
					wrong(column, leadership);
				}
			}
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
