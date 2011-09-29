package com.streambase.sb.sbfit.common.util;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import net.notwaving.vdc.ValueDateCache;
import net.notwaving.vdc.refdata.RefData;
import net.notwaving.vdc.refdata.RefDataBuilder;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

public class ValueDateUtil {
	private static ValueDateUtil instance;
	private ValueDateCache sVdc;
	private DateTimeFormatter myFormat;

	public static ValueDateUtil getValueDateUtil() throws IOException
	{
		if (null == instance) instance = new ValueDateUtil();
		return instance;
	}
	
	private ValueDateUtil() throws IOException
	{
		File ccyData = new File("config/application/ccydata.csv");
		File ccyPairData = new File("config/application/ccypairdata.csv");
		File calendarData = new File("build/data/calendar.dat");
		
		RefData rd = RefDataBuilder.loadRefData(ccyData.getAbsolutePath(), calendarData.getAbsolutePath(), ccyPairData.getAbsolutePath());
		
		sVdc = new ValueDateCache(Arrays.asList("SP"), rd);
		myFormat = new DateTimeFormatterBuilder()
			.appendYear(4,4).appendLiteral('-')
			.appendMonthOfYear(2).appendLiteral('-')
			.appendDayOfMonth(2).appendLiteral(" 00:00:00.000+0000")
			.toFormatter();
		
	}

	public static DateTimeFormatter getFormatter() throws IOException {
		return getValueDateUtil().myFormat;
	}

	public static ValueDateCache getVdc() throws IOException {
		return getValueDateUtil().sVdc;
	}
}
