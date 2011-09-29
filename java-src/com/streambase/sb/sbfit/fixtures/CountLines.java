package com.streambase.sb.sbfit.fixtures;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streambase.sb.sbfit.common.SbConversation;
import com.streambase.sb.sbfit.common.util.LogOutputCapture;
import com.streambase.sb.sbfit.common.util.LogOutputCapture.LogReader;

import fitnesse.fixtures.TableFixture;

public class CountLines extends TableFixture {
	private static final Logger logger = LoggerFactory.getLogger(CountLines.class);
	
	private SbConversation conversation;
	private LogReader br;
	private Pattern[] rowPatterns;
	private long[] counts;

	@Override
	protected void doStaticTable(int rowCount) {
		String sbdAlias = args[0];
		boolean useOutFile = args[1].equalsIgnoreCase("out");
		boolean startFromBegining = false;
		if (args.length >= 3) startFromBegining=Boolean.parseBoolean(args[2]);
		String readerName = null;
		if (args.length >= 4) readerName=args[3];
		
		try {
			conversation = SbClientFactory.getByAlias(sbdAlias);
			br = useOutFile ? LogOutputCapture.getCapturer().getOutReader(startFromBegining,readerName) : LogOutputCapture.getCapturer().getErrReader(startFromBegining,readerName);
			rowPatterns = new Pattern[rowCount];
			counts = new long[rowCount];
			
			setUpPatterns(rowCount);		
			readLinesAndMatch(rowCount);
			verifyCounts(rowCount);
		} catch (Exception e) {
			logger.debug("Count Lines", e);
        	exception(firstRow, e);
		}
	}

	private void verifyCounts(int rowCount) {
		for (int i=0;i < rowCount;i++)
		{
			long expectedCount = Long.parseLong(getCell(i, 1).text());
			long actualCount = counts[i];
			
			if (actualCount == expectedCount)
			{
				right(i,1);
			} else {					
				wrong(i,1,""+actualCount);
			}
		}
	}

	private void readLinesAndMatch(int rowCount) throws IOException {
		String line;
		while ((line = br.readLine()) != null)
		{
			for (int i=0;i < rowCount;i++)
			{
				Matcher m = rowPatterns[i].matcher(line);
				if (m.find()) counts[i]++;
			}
		}
	}

	private void setUpPatterns(int rowCount) {
		for (int i=0;i < rowCount;i++)
		{
			String regex = getCell(i, 0).text();
			
			for (String variable :conversation.getVariableNames())
			{
				Pattern p = Pattern.compile("&"+variable);
				regex = p.matcher(regex).replaceAll(conversation.getVariableValue(variable));
			}
			getCell(i, 0).body = regex;
			
			rowPatterns[i] = Pattern.compile(regex);
			counts[i] = 0;
		}
	}

}
