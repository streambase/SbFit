package com.streambase.sb.sbfit.fixtures;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streambase.sb.StreamBaseException;
import com.streambase.sb.sbfit.common.SbConversation;
import com.streambase.sb.sbfit.common.util.LogOutputCapture;
import com.streambase.sb.sbfit.common.util.LogOutputCapture.LogReader;

import fit.Parse;
import fitnesse.fixtures.TableFixture;

public class OrderedGrep extends TableFixture {
	private static final Logger logger = LoggerFactory.getLogger(OrderedGrep.class);

	@Override
	protected void doStaticTable(int rowCount) {
		String sbdAlias = args[0];
		LogReader br = null;
		try {
			SbConversation conversation = SbClientFactory.getByAlias(sbdAlias);

			boolean useOutFile = args[1].equalsIgnoreCase("out");
			boolean startFromBegining = false;
			if (args.length >= 3) startFromBegining=Boolean.parseBoolean(args[2]);
			String readerName = null;
			if (args.length >= 4) readerName=args[3];
			br = useOutFile ? LogOutputCapture.getCapturer().getOutReader(startFromBegining,readerName)
					: LogOutputCapture.getCapturer().getErrReader(startFromBegining,readerName);

			String line;
			for (int i = 0; i < rowCount; i++) {
				String regex = getCell(i, 0).text();
				
				for (String variable :conversation.getVariableNames())
				{
					Pattern p = Pattern.compile("&"+variable);
					regex = p.matcher(regex).replaceAll(conversation.getVariableValue(variable));
				}
				getCell(i, 0).body = regex;
				
				Pattern p = Pattern.compile(regex);
				boolean foundLine = false;
				while ((line = br.readLine()) != null) {
					Matcher m = p.matcher(line);
					if (m.find()) {
						foundLine = true;
						right(i, 0);
						
						Parse row = getCell(i, 0);
						int group = 1;
						while (null != (row=row.more))
						{
							if (m.groupCount() >= group) 
							{
								String text = row.text();
								String actual = m.group(group);
								String varName = text.substring(1);
								if (text.startsWith("$")) 
								{
									conversation.defineVariable(varName, actual);
								} 
								else if (text.startsWith("&"))
								{
									String expected = conversation.getVariableValue(varName);
									if (actual.equals(expected))
										right(i,group);
									else 
										wrong(i,group,"Expected=\""+expected+"\", Actual=\""+actual+"\"");
								}
							} else {
								wrong(i,group,"No group available");
							}
							group++;
						}
						break;
					}
				}
				if (!foundLine) wrong(i, 0,"Not Found");
			}
		} catch (FileNotFoundException e) {
			logger.info("Ordered Grep", e);
			exception(firstRow, e);
		} catch (IOException e) {
			logger.info("Ordered Grep", e);
			exception(firstRow, e);
		} catch (StreamBaseException e) {
			logger.info("Ordered Grep", e);
			exception(firstRow, e);
		}
	}

}
