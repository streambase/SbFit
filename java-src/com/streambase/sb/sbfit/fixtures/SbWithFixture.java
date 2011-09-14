/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import java.util.ArrayList;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.streambase.sb.sbfit.common.SbConversation;
import com.streambase.sb.sbfit.common.util.ProcessRegistry;

import com.streambase.sb.DataType;
import com.streambase.sb.Schema;
import com.streambase.sb.StreamBaseException;
import com.streambase.sb.StreamProperties;
import com.streambase.sb.Tuple;
import com.streambase.sb.operator.TypecheckException;
import com.streambase.sb.unittest.Dequeuer;
import com.streambase.sb.unittest.internal.embedded.EmbeddedServerManager;

import fit.Binding;
import fit.Fixture;
import fit.Parse;
import com.streambase.sb.sbfit.common.util.*;

public class SbWithFixture implements SbFixtureMixin {
	protected Fixture target = null;
	protected String alias = null;
	protected SbConversation conversation = null;
	protected String streamName = null;
	protected Schema schema = null;
	protected Tuple pivot = null;
	protected Binding[] bindings;
	protected String[] bindingFieldNames;
	private SbFixtureType type = null;
	private double double_compare_epsilon = 0.00001;
	private long timeout = 1000;

	public SbWithFixture(Fixture target, SbFixtureType type) {
		this.target = target;
		this.type = type;
	}

	public Parse doArgs(Parse rows, String... args) throws StreamBaseException {
		this.alias = args[0];
		this.conversation = SbClientFactory.getByAlias(alias);
		if (SbFixtureType.Blast.equals(type)
				|| SbFixtureType.SbdStart.equals(type)
				|| SbFixtureType.SbdStop.equals(type)
				|| SbFixtureType.DefineVariable.equals(type)) {
			return rows;
		} else {

			if ((streamName == null || streamName.trim().length() == 0)
					&& args.length > 0) {
				setStreamName(args[1]);
				if (SbFixtureType.BlockingDequeue.equals(type)) {
					timeout = Long.valueOf(args[2]);
					if (args.length > 3) {
						double_compare_epsilon = Double.valueOf(args[3]);
					}
				}
				return rows;
			} else if (streamName == null) {
				setStreamName(rows.parts.text());
				if (SbFixtureType.BlockingDequeue.equals(type)) {
					timeout = Long.valueOf(args[2]);
					if (args.length > 3) {
						double_compare_epsilon = Double.valueOf(args[3]);
					}
				}
				return rows;
			}

		}
		throw new IllegalStateException("Bug!");
	}

	private void setStreamName(String streamName) throws StreamBaseException {
		this.streamName = streamName;
	}

	public void initBindings(Parse headerCells) throws StreamBaseException {
		conversation = SbClientFactory.getByAlias(alias);
		schema = conversation.getSchemaForStream(streamName);

		//EmbeddedServerManager sbd = ProcessRegistry.get(alias);
		//schema = sbd.getDequeuer( streamName ).getSchema();
		//if ( schema != null ) {
		//	schema = sbd.getEnqueuer( streamName ).getSchema();
		//}
		pivot = schema.createTuple();
		String[] key = new String[headerCells.size()];
		bindings = new Binding[headerCells.size()];
		bindingFieldNames = new String[headerCells.size()];

		for (int i = 0; headerCells != null; i++, headerCells = headerCells.more) {
			String text = headerCells.text();
			String name = text;
			// boolean isKey = false;
			if (text.startsWith("*")) {
				// isKey = true;
				name = text.substring(1);
				key[i] = name;
			}
			if (!schema.hasField(name)) {
				wrong(headerCells);
				throw new TypecheckException("Cannot find schema column "
						+ name + " for stream " + streamName);
			}
			bindingFieldNames[i] = name;
			bindings[i] = new Binding.SetBinding(); // isEnqueue ? new
													// Binding.SetBinding() :
													// new
													// Binding.QueryBinding();
			bindings[i].adapter = new SbTypeAdapter(this, name,
					type == SbFixtureType.Enqueue);
		}
	}

	public void initBindingsWithExclusions(Parse headerCells)
			throws StreamBaseException {
		conversation = SbClientFactory.getByAlias(alias);
		schema = conversation.getSchemaForStream(streamName);
		pivot = schema.createTuple();

		List<String> fieldList = new ArrayList<String>();
		for (; headerCells != null; headerCells = headerCells.more) {
			String text = headerCells.text();
			String name = text;
			// boolean isKey = false;
			if (text.startsWith("*")) {
				// isKey = true;
				name = text.substring(1);
			}
			if (!name.startsWith("^")) {
				if (!schema.hasField(name)) {
					wrong(headerCells);
					throw new TypecheckException("Cannot find schema column "
							+ name + " for stream " + streamName);
				}
				fieldList.add(name);
			}
		}
		bindings = new Binding[fieldList.size()];
		bindingFieldNames = new String[fieldList.size()];
		for (int i = 0; i < bindings.length; i++) {
			String name = fieldList.get(i);
			bindingFieldNames[i] = name;
			bindings[i] = new Binding.SetBinding(); // isEnqueue ? new
													// Binding.SetBinding() :
													// new
													// Binding.QueryBinding();
			bindings[i].adapter = new SbTypeAdapter(this, name,
					type == SbFixtureType.Enqueue);
		}
	}

	public void wrong(Parse parse) {
		target.wrong(parse);
	}

	public void right(Parse parse) {
		target.right(parse);
	}

	public void exception(Parse parse, Throwable t) {
		target.exception(parse, t);
	}

	public void enqueue(Parse rows) throws Throwable {
		Parse row = rows;
		while ((row = row.more) != null) {
			enqueueRow(row);
		}
	}

	private void processVariableCell(Parse cell, String value) throws Throwable {
		String text = cell.text();

		String variableName = null;
		String calculation = null;

		Pattern pattern = Pattern.compile("[+-/*]\\d++");
		Matcher matcher = pattern.matcher(text);
		if (matcher.find()) {
			calculation = matcher.group();
			variableName = text.substring(1, matcher.start());
		} else {
			variableName = text.substring(1);
		}

		if (value != null) {// set the variable to the specified value
			conversation.resetVariable(variableName, value);
		}

		String variableValue = conversation.getVariableValue(variableName);

		if (calculation != null)// some calculation needs to be done
		{
			char operator = calculation.charAt(0);
			long operand = Long.parseLong(calculation.substring(1));

			switch (operator) {
			case '+':
				variableValue = String.valueOf(Long.parseLong(variableValue)
						+ operand);
				break;
			case '-':
				variableValue = String.valueOf(Long.parseLong(variableValue)
						- operand);
				break;
			default:// multiplication and devision yet to be implemented
			}
		}
		cell.body = String.valueOf(variableValue);
	}

	public void enqueueRow(Parse row) throws Throwable {
		assert row != null;
		pivot.clear();
		Parse cell = row.parts;

		for (int column = 0; column < bindings.length; column++, cell = cell.more) {
			if (cell.text().startsWith("$")) {
				processVariableCell(cell, null);
			}
			bindings[column].doCell(target, cell);
		}
		conversation.enqueue(streamName, pivot.clone());
		target.right(row);
	}

	public void defineVariable(Parse rows) throws Throwable {
		Parse row = rows;
		while ((row = row.more) != null) {
			Parse cell = row.parts;
			String variableName = cell.text();
			String variableValue = cell.more.text();
			conversation.defineVariable(variableName, variableValue);
			target.right(row);
		}
	}

	public void dequeue(Parse rows) throws Throwable {
		Parse row = rows;
		int count = 0;
		EmbeddedServerManager sbd = ProcessRegistry.get(alias);

		Dequeuer d = sbd.getDequeuer(streamName);
		List<Tuple> tuples = d.dequeue(rows.size() - 1, 100, TimeUnit.MILLISECONDS);
		StreamProperties streamProps = d.getStreamProperties();
		for (Tuple t : tuples) {
			if (!streamProps.getName().equals(streamName)) {
				continue;
			}
			count++;
			pivot = t;
			if (row == null) {
				break;
			}

			// burn the header
			row = row.more;
			dequeueRow(row, false);
		}

		tuples = d.dequeue(-1, 0, TimeUnit.SECONDS);
		for (Tuple t : tuples) {
			addUnexpectedRow(rows.last(), t);
		}

		while (row != null && (row = row.more) != null) {
			wrong(row);
		}
	}

	public void blockingDequeue(Parse rows, boolean resetVariable)
			throws Throwable {
		Parse row = rows;
		EmbeddedServerManager sbd = ProcessRegistry.get(alias);
		Dequeuer d = sbd.getDequeuer(streamName);
		while (row != null && (row = row.more) != null) {
			List<Tuple> t = d.dequeue(1, timeout, TimeUnit.MILLISECONDS);
			if (t.isEmpty()) {
				wrong(row);
				System.out.println(System.currentTimeMillis()
						+ " timed out waiting for dequeue: " + timeout);
				break;
			}
			pivot = t.get(0);
			System.out.println(System.currentTimeMillis() + " recieved:"
					+ pivot);
			dequeueRow(row, resetVariable);
		}
	}

	public void blockingDequeueWithExpects(Parse rows) throws Throwable {
		Parse row = rows;
		EmbeddedServerManager sbd = ProcessRegistry.get(alias);
		Dequeuer d = sbd.getDequeuer(streamName);
		while (row != null && (row = row.more) != null) {
			List<Tuple> t = d.dequeue(1, timeout, TimeUnit.MILLISECONDS);
			SbAnswerActionType action = howToDealWithAnswer(row);
			if (t.isEmpty()) {
				System.out.println(System.currentTimeMillis()
						+ " timed out waiting for dequeue: " + timeout);
				if (action == SbAnswerActionType.NotExpected) {
					right(row);
					continue;
				} else {
					wrong(row);
					break;
				}
			} else {
				System.out.println(System.currentTimeMillis() + " recieved:"
						+ pivot);
				if (action == SbAnswerActionType.NotExpected) {
					wrong(row);
					break;
				} else if (action == SbAnswerActionType.Ignore) {
					right(row);
					continue;
				}
			}
			pivot = t.get(0);
			dequeueRow(row, false);
		}
	}

	private void addUnexpectedRow(Parse priorRow, Tuple t) {
		Parse lastRow = priorRow;
		Parse newRow = new Parse("tr", null, null, null);
		lastRow.more = newRow;
		lastRow = newRow;
		try {
			Parse cell = new Parse("td", "", null, null);
			cell.addToBody(Fixture.gray("? = " + t.getField(0)));
			wrong(cell);
			newRow.parts = cell;
			for (int column = 1; column < bindings.length; column++) {
				Parse current = new Parse("td", "", null, null);
				current.addToBody(Fixture.gray("? = " + t.getField(column)));
				wrong(current);
				cell.more = current;
				cell = current;
			}
		} catch (Exception e) {
			exception(newRow, e);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	private void dequeueRow(Parse row, boolean resetVariable) throws Throwable {
		Parse cell = row.parts;

		for (int column = 0; column < bindings.length; column++, cell = cell.more) {
			String fieldName = bindingFieldNames[column];
			Schema.Field actual = pivot.getSchema().getField(fieldName);
			Object actualValue = pivot.getField(actual);

			if (cell.text().startsWith("$")) {
				if (resetVariable) {
					processVariableCell(cell, actualValue.toString());
				} else {
					processVariableCell(cell, null);
				}
			}
			bindings[column].doCell(target, cell);
			String expected = cell.text();

			Object expectedValue = null;

			if (actual.getDataType() == DataType.LIST) {
				expectedValue = SbTypeAdapter.convertList(actual
						.getElementType(), expected);
			}  if (actual.getDataType() == DataType.TUPLE) {
				expectedValue = com.streambase.sb.unittest.CSVTupleMaker.MAKER.createTuple( actual.getSchema(), expected );
			} else {
				expectedValue = SbTypeAdapter.convert(actual.getDataType(),
						expected);
			}

			if (expectedValue == null || actualValue == null) {
				if (actualValue == expectedValue) {
					right(cell);
					continue;
				}
			} else if (expectedValue.equals(actualValue)) {
				right(cell);
				continue;
			} else {
				if (actual.getDataType() == DataType.DOUBLE) {
					Double diff = ((Double) actualValue)
							- ((Double) expectedValue);
					if (Math.abs(diff) <= double_compare_epsilon) {
						right(cell);
						continue;
					}
				} else if (expected.equalsIgnoreCase("blank")) {
					if (actualValue.toString().length() == 0) {
						right(cell);
						continue;
					}
				}
			}
			wrong(cell);
			cell.addToBody(Fixture.gray(" = " + actualValue));
		}
	}

	private SbAnswerActionType howToDealWithAnswer(Parse row) throws Throwable {
		SbAnswerActionType action = SbAnswerActionType.Check;
		Parse cell = row.parts;
		for (; cell != null; cell = cell.more) {
			String text = cell.text();
			if (text.startsWith("^")) {
				if (text.equalsIgnoreCase("^n")) {
					action = SbAnswerActionType.NotExpected;
				} else if (text.equalsIgnoreCase("^y")) {
					action = SbAnswerActionType.Check;
				} else {
					action = SbAnswerActionType.valueOf(text.substring(1));
				}
				break;
			}
		}
		return action;
	}

	public void startSbd(String alias, String app) throws Exception {
		EmbeddedServerManager sbd = EmbeddedServerCache.getCache().getNewEmbeddedServer( app );
		ProcessRegistry.register(alias, sbd);
	}

	public void stopSbd(String alias) throws Exception {
		EmbeddedServerManager sbd = ProcessRegistry.get(alias);
		sbd.stopContainers();
		ProcessRegistry.remove(alias);
		SbClientFactory.close(alias);
	}
}
