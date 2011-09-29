package com.streambase.sb.sbfit.fixtures;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streambase.sb.sbfit.common.util.LogOutputCapture;
import com.streambase.sb.sbfit.common.util.LogOutputCapture.LogReader;

import fit.Parse;
import fitnesse.fixtures.TableFixture;

public class PrintLog extends TableFixture {
	private static final Logger logger = LoggerFactory.getLogger(PrintLog.class);

	@Override
	protected void doStaticTable(int rowCount) {
		LogReader br = null;
		try {
			boolean startFromBegining = false;
			if (args.length >= 2) startFromBegining=Boolean.parseBoolean(args[1]);
			String readerName = null;
			if (args.length >= 3) readerName=args[2];
			boolean useOutFile = args[0].equalsIgnoreCase("out");
			br = useOutFile ? LogOutputCapture.getCapturer().getOutReader(startFromBegining,readerName)
					: LogOutputCapture.getCapturer().getErrReader(startFromBegining,readerName);

			String line; int i = 0;
			while ((line = br.readLine()) != null) {
				i++;
				addRow(getCell(0,0).last(),line);
			}
		} catch (FileNotFoundException e) {
			logger.debug("Print Log", e);
			exception(firstRow, e);
		} catch (IOException e) {
			logger.debug("Print Log", e);
			exception(firstRow, e);
		}
	}
	private void addRow(Parse priorRow, String t) {
		Parse lastRow = priorRow;
		Parse newRow = new Parse("tr", null, null, null);
		lastRow.more = newRow;
		lastRow = newRow;
		try {
			Parse cell = new Parse("td", "", null, null);
			cell.addToBody(t);
			wrong(cell);
			newRow.parts = cell;
		} catch (Exception e) {
			exception(newRow, e);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

}
