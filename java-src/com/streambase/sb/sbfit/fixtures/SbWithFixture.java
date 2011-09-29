/*
 * Copyright (c) 2004-2008 StreamBase Systems, Inc. All rights reserved.
 * 
 */
package com.streambase.sb.sbfit.fixtures;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.notwaving.vdc.ValueDate;
import net.notwaving.vdc.ValueDateCache;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streambase.sb.DataType;
import com.streambase.sb.Schema;
import com.streambase.sb.StreamBaseException;
import com.streambase.sb.Tuple;
import com.streambase.sb.operator.TypecheckException;
import com.streambase.sb.sbfit.common.SbConversation;
import com.streambase.sb.sbfit.common.util.EmbeddedServerCache;
import com.streambase.sb.sbfit.common.util.ProcessRegistry;
import com.streambase.sb.sbfit.common.util.SchemaFieldColumnMapper;
import com.streambase.sb.sbfit.common.util.ValueDateUtil;
import com.streambase.sb.unittest.CSVTupleMaker;
import com.streambase.sb.unittest.Dequeuer;
import com.streambase.sb.unittest.Expecter;
import com.streambase.sb.unittest.FieldBasedTupleComparator;
import com.streambase.sb.unittest.SBServerManager;
import com.streambase.sb.unittest.TupleComparator;
import com.streambase.sb.util.Util;

import fit.Binding;
import fit.Fixture;
import fit.Parse;

public class SbWithFixture implements SbFixtureMixin {
    private static final Logger logger = LoggerFactory.getLogger(SbWithFixture.class);

    enum PostDequeueMode {
        WAIT_ERROR, CHECK_IGNORE, CHECK_ERROR, DONT_CHECK,
    }

    protected Fixture target = null;
    protected String alias = null;
    protected SbConversation conversation = null;
    protected String streamName = null;
    protected Schema schema = null;
    protected Tuple pivot = null;
    protected Binding[] bindings;
    protected String[] bindingFieldNames;
    private SbFixtureType type = null;
    private double doubleCompareEpsilon = Util.DEFAULT_DOUBLE_COMPARE_TOLERANCE;
    private long timeout = -1;
    private long perLineTimeout = 500;
    private PostDequeueMode postDeqMode = PostDequeueMode.CHECK_ERROR;

    public SbWithFixture(Fixture target, SbFixtureType type) {
        this.target = target;
        this.type = type;
    }

    public void start() {
        SbFixtureReporter.reporter.start(type.toString());
    }

    public void stop() {
        SbFixtureReporter.reporter.stop(type.toString());
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
                    setTimeout(Long.valueOf(args[2]));
                    if (args.length > 3) {
                        doubleCompareEpsilon = Double.valueOf(args[3]);
                    }
                }
                return rows;
            } else if (streamName == null) {
                setStreamName(rows.parts.text());
                if (SbFixtureType.BlockingDequeue.equals(type)) {
                    setTimeout(Long.valueOf(args[2]));
                    if (args.length > 3) {
                        doubleCompareEpsilon = Double.valueOf(args[3]);
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
            logger.debug("field: {}", name);
            
            bindings[i] = new Binding.SetBinding(); // isEnqueue ? new
                                                    // Binding.SetBinding() :
                                                    // new
                                                    // Binding.QueryBinding();
            bindings[i].adapter = new SbTypeAdapter(this, name,
                    type == SbFixtureType.Enqueue);
        }
    }

    public void initBindingsWithExclusions(Parse headerCells) throws StreamBaseException {
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

    private String processVariable(String variable, String actualValue,
            boolean set) {
        String variableName = null;
        String calculation = null;

        Pattern pattern = Pattern.compile("[+-/*]\\d++");
        Matcher matcher = pattern.matcher(variable);
        if (matcher.find()) {
            calculation = matcher.group();
            variableName = variable.substring(1, matcher.start());
        } else {
            variableName = variable.substring(1);
        }

        if (set) {
            conversation.resetVariable(variableName, actualValue);
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
        return String.valueOf(variableValue);
    }

    public void enqueueRow(Parse row) throws Throwable {
        assert row != null;
        pivot.clear();
        Parse cell = row.parts;

        for (int column = 0; column < bindings.length; column++, cell = cell.more) {

            // This must be a variable write because we cannot set variables in
            // an enqueue

            if (cell.text().contains("&")) {
                String[] cellTextTokens = cell.text().split(",");
                String tempCellBody = "";
                for (int i = 0; i < cellTextTokens.length; i++) {
                    String endCellBody = "";
                    while (cellTextTokens[i].startsWith("[")) {
                        tempCellBody = tempCellBody + "[";
                        cellTextTokens[i] = cellTextTokens[i].substring(1);
                    }
                    while (cellTextTokens[i].startsWith("\"")) {
                        tempCellBody = tempCellBody + "\"";
                        cellTextTokens[i] = cellTextTokens[i].substring(1);
                    }
                    while (cellTextTokens[i].endsWith("]")) {
                        endCellBody = "]" + endCellBody;
                        cellTextTokens[i] = cellTextTokens[i].substring(0,
                                cellTextTokens[i].length() - 1);
                    }
                    while (cellTextTokens[i].endsWith("\"")) {
                        endCellBody = "\"" + endCellBody;
                        cellTextTokens[i] = cellTextTokens[i].substring(0,
                                cellTextTokens[i].length() - 1);
                    }
                    if (cellTextTokens[i].startsWith("&")) {
                        cellTextTokens[i] = processVariable(cellTextTokens[i],
                                null, false);
                        if (cellTextTokens[i].length() == 0
                                && cellTextTokens.length > 1) {
                            cellTextTokens[i] = "null";
                        }
                    }
                    tempCellBody = tempCellBody + cellTextTokens[i]
                            + endCellBody + ",";
                }
                // Remove last char which will be ","
                tempCellBody = tempCellBody.substring(0,
                        tempCellBody.length() - 1);
                cell.body = tempCellBody;
            }
            bindings[column].doCell(target, cell);
        }
        conversation.enqueue(streamName, pivot.clone());
    }

    public String getVariable(String name) throws Throwable {
        return conversation.getVariableValue(name);
    }

    public void defineVariable(Parse rows) throws Throwable {
        Parse row = rows;
        while (row != null) {
            Parse cell = row.parts;
            String variableName = cell.text();
            String variableValue = cell.more.text();
            conversation.defineVariable(variableName, variableValue);
            target.right(row);
            row = row.more;
        }
    }

    public void blockingDequeueWithExpects(Parse rows) throws Throwable {

        if (!SbConversation.isTestMode())
            return;

        // burn the header row
        Parse row = rows.more;

        SBServerManager sbd = ProcessRegistry.get(alias);
        Dequeuer d = sbd.getDequeuer(streamName);

        while (row != null) {
            List<Tuple> t = d.dequeue(1, getTimeout(false), TimeUnit.MILLISECONDS);
            SbAnswerActionType action = howToDealWithAnswer(row);
            if (t.isEmpty()) {
                if (action == SbAnswerActionType.NotExpected) {
                    right(row);
                    continue;
                } else {
                    wrong(row);
                    break;
                }
            } else {
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
            row = row.more;
        }

    }

    private void addUnexpectedRow(Parse priorRow, Tuple t) {
        Parse lastRow = priorRow;
        Parse newRow = new Parse("tr", null, null, null);
        lastRow.more = newRow;
        lastRow = newRow;
        try {
            Parse cell = new Parse("td", "", null, null);
            String fieldName = bindingFieldNames[0];
            cell.addToBody(Fixture.gray("? = " + t.getField(fieldName)));
            target.ignore(cell);
            newRow.parts = cell;
            for (int column = 1; column < bindings.length; column++) {
                fieldName = bindingFieldNames[column];
                Parse current = new Parse("td", "", null, null);
                current.addToBody(Fixture.gray("? = " + t.getField(fieldName)));
                target.ignore(current);
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
        if (!SbConversation.isTestMode())
            return;

        Parse cell = row.parts;

        for (int column = 0; column < bindings.length; column++, cell = cell.more) {
            String fieldName = bindingFieldNames[column];
            Schema.Field actual = pivot.getSchema().getField(fieldName);
            Object actualValue = pivot.getField(actual);
            if (actualValue instanceof String)
                actualValue = ((String) actualValue).trim();

            // Multilayered variable replacement - only works for basic varibles
            // i.e. not lists or tuples.
            
            logger.info("Dequeue: {}", cell.text());

            if (cell.text().contains("$") || cell.text().contains("&")) {
                String[] cellTextTokens = cell.text().split(",");
                String[] actualTokens = { "" };
                if (actualValue != null) {
                    actualTokens = actualValue.toString().split(",",
                            cellTextTokens.length);
                }

                if (cellTextTokens.length != actualTokens.length) {
                    throw new IllegalStateException(
                            "You must declare a match value or variable for each token returned");
                }

                String tempCellBody = "";
                for (int i = 0; i < cellTextTokens.length; i++) {
                    String endCellBody = "";
                    while (cellTextTokens[i].startsWith("[")) {
                        if (actualTokens[i].startsWith("[")) {
                            tempCellBody = tempCellBody + "[";
                            cellTextTokens[i] = cellTextTokens[i].substring(1);
                            actualTokens[i] = actualTokens[i].substring(1);
                        } else {
                            throw new IllegalStateException(
                                    "You must match the list and tuple structure required");
                        }
                    }
                    while (cellTextTokens[i].startsWith("\"")) {
                        if (actualTokens[i].startsWith("\"")) {
                            tempCellBody = tempCellBody + "\"";
                            cellTextTokens[i] = cellTextTokens[i].substring(1);
                            actualTokens[i] = actualTokens[i].substring(1);
                        } else {
                            throw new IllegalStateException(
                                    "You must match the list and tuple structure required");
                        }
                    }
                    while (cellTextTokens[i].endsWith("]")) {
                        if (actualTokens[i].endsWith("]")) {
                            endCellBody = "]" + endCellBody;
                            cellTextTokens[i] = cellTextTokens[i].substring(0,
                                    cellTextTokens[i].length() - 1);
                            actualTokens[i] = actualTokens[i].substring(0,
                                    actualTokens[i].length() - 1);
                        } else {
                            throw new IllegalStateException(
                                    "You must match the list and tuple structure required");
                        }
                    }
                    while (cellTextTokens[i].endsWith("\"")) {
                        if (actualTokens[i].endsWith("\"")) {
                            endCellBody = "\"" + endCellBody;
                            cellTextTokens[i] = cellTextTokens[i].substring(0,
                                    cellTextTokens[i].length() - 1);
                            actualTokens[i] = actualTokens[i].substring(0,
                                    actualTokens[i].length() - 1);
                        } else {
                            throw new IllegalStateException(
                                    "You must match the list and tuple structure required");
                        }
                    }
                    if (cellTextTokens[i].startsWith("$")) {
                        // Process
                        if (resetVariable) {
                            cellTextTokens[i] = processVariable(
                                    cellTextTokens[i], actualTokens[i], true);
                        } else {
                            cellTextTokens[i] = processVariable(
                                    cellTextTokens[i], null, false);
                        }
                    } else if (cellTextTokens[i].startsWith("&")) {
                        // Must be a variable get
                        cellTextTokens[i] = processVariable(cellTextTokens[i],
                                null, false);
                    }
                    tempCellBody = tempCellBody + cellTextTokens[i]
                            + endCellBody + ",";
                }
                // Remove last char which will be ","
                tempCellBody = tempCellBody.substring(0,
                        tempCellBody.length() - 1);
                cell.body = tempCellBody;
            }

            // See if it's empty - if it's defaulting

            boolean isEmpty = false;
            if (cell.text().isEmpty())
                isEmpty = true;

            bindings[column].doCell(target, cell);
            String expected = cell.text();
            Object expectedValue = null;

            if (actual.getDataType() == DataType.LIST) {
                expectedValue = SbTypeAdapter.convertList(
                        actual.getElementType(), expected);
            } else if (actual.getDataType() == DataType.TUPLE) {
                expectedValue = expected.equals("null") ? null
                        : com.streambase.sb.unittest.CSVTupleMaker.MAKER
                                .createTuple(actual.getSchema(), expected);
            } else {
                expectedValue = SbTypeAdapter.convert(actual.getDataType(),
                        expected);
            }

            if (expectedValue == null || actualValue == null) {
                if (actualValue == expectedValue) {
                    if (!isEmpty)
                        right(cell);
                    continue;
                }
            } else if (expectedValue.equals(actualValue)) {
                // If it's not empty, then it's right ... otherwise leave it
                // alone.
                if (!isEmpty)
                    right(cell);
                continue;
            } else {
                if (actual.getDataType() == DataType.DOUBLE) {
                    if (Util.compareDoubles(doubleCompareEpsilon,
                            (Double) actualValue, (Double) expectedValue)) {
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

    public void startSbd(String alias, String app, String containerName,
            Map<String, String> params, Integer port, Integer hbPort,
            Integer peerHbPort) throws Exception {

        if (!SbConversation.isTestMode())
            return;

        if (ProcessRegistry.contains(alias))
            throw new StreamBaseException("Can't start " + alias
                    + "without stopiing it first");
        
        SBServerManager sbd = null;
        
        try {
        	sbd = EmbeddedServerCache.getCache().getNewEmbeddedServer(containerName, params, port, hbPort, peerHbPort, app);
        }
        catch(Exception e) {
        	logger.error("Exception starting sbd: {}", e);
        	throw e;
        }
        catch(Throwable t) {
        	logger.error("Throwable starting sbd: {}", t);
        	throw new Exception(t);
        }
        
        ProcessRegistry.register(alias, sbd);
    }

    public void stopSbd(String sbdAlias) throws Exception {

        if (!SbConversation.isTestMode())
            return;

        SBServerManager sbd = ProcessRegistry.get(sbdAlias);
        sbd.stopContainers();
        reallyStopSbd(sbdAlias);
    }

	private void reallyStopSbd(String sbdAlias) throws StreamBaseException {
		ProcessRegistry.remove(sbdAlias);
        SbClientFactory.close(sbdAlias);
	}

    public void defineVariableUsingValuedateCalc(Parse rows) throws IOException {
        Parse row = rows;
        while (row != null) {
            Parse cell = row.parts;
            String variableName = cell.text();
            String variableCurrency1 = cell.more.text();
            String variableCurrency2 = cell.more.more.text();

            ValueDateCache vdc = ValueDateUtil.getVdc();
            ValueDate valueDate = vdc.valueDate(
                    new DateTime(System.currentTimeMillis(), DateTimeZone.UTC),
                    "SP", variableCurrency1, variableCurrency2);

            String vDateString = valueDate.getVDate().toString(
                    ValueDateUtil.getFormatter());
            conversation.defineVariable(variableName, vDateString);
            target.right(row);
            row = row.more;
        }
    }

    public Parse doDequeueArgs(Parse rows, String[] args) throws Exception {
        if (!(2 <= args.length && args.length <= 6)) {
            throw new Exception(
                    "Incorrect arguments for Dequeue, the correct arguments should be: \n"
                            + " 1. Alias (mytest) \n" + " 2. Stream to use \n"
                            + " 3. Dequeue timeout \n"
                            + " 4. Continuation mode \n"
                            + " 5. Double epsilon \n"
                            + " 6. Per-line timeout \n "
                            + "More details can be found on the Wiki");
        }

        alias = args[0];
        conversation = SbClientFactory.getByAlias(alias);
        setStreamName(args[1]);
        if (args.length > 2) {
            String val = args[2];
            if (!val.equals("0")) {
                perLineTimeout = setTimeout(Long.parseLong(val));
            }
        }
        if (args.length > 3) {
            String mode = args[3];

            postDeqMode = null;
            // handle legacy names
            if (mode.equalsIgnoreCase("continued")) {
                postDeqMode = PostDequeueMode.DONT_CHECK;
            } else if (mode.equalsIgnoreCase("accept")) {
                postDeqMode = PostDequeueMode.CHECK_IGNORE;
            } else if (mode.equalsIgnoreCase("normal")) {
                postDeqMode = PostDequeueMode.CHECK_ERROR;
            } else {
                for (PostDequeueMode m : PostDequeueMode.values()) {
                    if (m.toString().equalsIgnoreCase(mode)) {
                        postDeqMode = m;
                    }
                }
            }

            if (postDeqMode == null) {
                throw new Exception(
                        MessageFormat
                                .format("Unsupported continuation mode {0} valid modes are {1}",
                                        mode,
                                        Util.join(", ",
                                                PostDequeueMode.values())));
            }
        }

        if (args.length > 4) {
            String val = args[4];
            if (!val.equals("0")) {
                perLineTimeout = Long.parseLong(val);
            }
        }
        if (args.length > 5) {
            if (!args[5].equals("0")) {
                doubleCompareEpsilon = Double.valueOf(args[5]);
            }
        }

        return rows;
    }

    public void newDequeue(Parse rows) throws Throwable {
        int expectedRows = rows.size() - 1;
        int returnedRows = 0;
        long endTime = System.currentTimeMillis() + getTimeout(false);
        List<Tuple> tuples = null;
        SBServerManager sbd = ProcessRegistry.get(alias);
        Dequeuer d = sbd.getDequeuer(streamName);
        Parse row = rows;

        logger.debug("about to dequeue {} tuples", expectedRows);
        try {
            SbFixtureReporter.reporter.start(SbFixtureType.Dequeue + " - main");
            while (returnedRows < expectedRows && (row = row.more) != null) {

                tuples = d.dequeue(1, perLineTimeout, TimeUnit.MILLISECONDS);

                if (tuples.isEmpty()) {
                    wrong(row);
                    break;
                }
                returnedRows++;
                pivot = tuples.get(0);
                dequeueRow(row, true);

                if (System.currentTimeMillis() > endTime) {
                    throw new StreamBaseException("Timed Out");
                }
            }
        } finally {
            SbFixtureReporter.reporter.stop(SbFixtureType.Dequeue + " - main");
        }

        boolean checkTuples;
        if (expectedRows == 0) {
            try {
                SbFixtureReporter.reporter.start(SbFixtureType.Dequeue + " - empty");
                tuples = d.dequeue(-1, getTimeout(true), TimeUnit.MILLISECONDS);
                if (tuples.size() == 0) {
                    right(rows);
                } else {
                    wrong(rows);
                }
                checkTuples = true;
            } finally {
                SbFixtureReporter.reporter.stop(SbFixtureType.Dequeue + " - empty");
            }
        } else {
            try {
                SbFixtureReporter.reporter.start(SbFixtureType.Dequeue + " - trailing");
                switch (postDeqMode) {
                case CHECK_ERROR:
                    tuples = d.dequeue(-1, 0, TimeUnit.SECONDS);
                    checkTuples = true;
                    break;
                case CHECK_IGNORE:
                    tuples = d.dequeue(-1, 0, TimeUnit.SECONDS);
                    checkTuples = false;
                    break;
                case WAIT_ERROR:
                    tuples = d.dequeue(-1,
                            endTime - System.currentTimeMillis(),
                            TimeUnit.MILLISECONDS);
                    checkTuples = true;
                    break;
                case DONT_CHECK:
                    checkTuples = false;
                    break;
                default:
                    throw new RuntimeException("Unhandled Post Dequeue Mode: "
                            + postDeqMode);
                }
            } finally {
                SbFixtureReporter.reporter.stop(SbFixtureType.Dequeue + " - trailing");
            }
        }

        if (checkTuples && !tuples.isEmpty()) {
            for (Tuple t : tuples) {
                addUnexpectedRow(rows.last(), t);
            }

            while (row != null && (row = row.more) != null) {
                wrong(row);
            }
        }
    }

    public void notInDequeue(Parse rows) throws Throwable {
        int expectedRows = rows.size() - 1;
        long endTime = System.currentTimeMillis() + getTimeout(false);
        SBServerManager sbd = ProcessRegistry.get(alias);
        Dequeuer d = sbd.getDequeuer(streamName);
        Schema schema = d.getSchema();
        Parse row = rows;

        logger.debug("about to NotInDequeue {} tuples", expectedRows);
        logger.info("unordered dequeue, header: {}", Arrays.asList(bindingFieldNames));

        TupleComparator comparator = new FieldBasedTupleComparator(FieldBasedTupleComparator.DEFAULT_COMPARATOR, bindingFieldNames); 
        SchemaFieldColumnMapper mapper = new SchemaFieldColumnMapper(d.getSchema(), bindingFieldNames);
        List<Tuple> notExpected = getTuplesFromTable(schema, row, mapper);
        List<Tuple> actual = dequeueTuples(d);
        List<Tuple> found = new ArrayList<Tuple>();
        List<Tuple> unmatched = new ArrayList<Tuple>();
        
        for(Tuple t : actual) {
        	boolean matched = false;
        	
        	for(Tuple not : notExpected) {
        		if(comparator.compare(not, t)) {
        			found.add(not);
        			matched = true;
        		}
        	}
        	
        	if(!matched)
        		unmatched.add(t);
        }
        
        if(found.size() == 0) {
        	markAllCorrect(rows);
        } else {
        	showNotInDequeueErrors(rows, schema, comparator, mapper, found, unmatched);
        }
    }
    
    /**
     * Dequeue all the tuples that are there.
     * 
     * TODO -- add a max time timeout to prevent the test from hanging
     */
    private List<Tuple> dequeueTuples(Dequeuer d) throws StreamBaseException {
    	List<Tuple> tuples = new ArrayList<Tuple>();
        List<Tuple> read = null;
        
        while(true) {
        	read = d.dequeue(1, perLineTimeout, TimeUnit.MILLISECONDS);
        	
        	if(read.size() > 0) {
        		tuples.addAll(read);
        	} else {
        		break;
        	}
        }
    	
    	return tuples;
    }
    
    public void unorderedDequeue(Parse rows) throws Throwable {
        int expectedRows = rows.size() - 1;
        long endTime = System.currentTimeMillis() + getTimeout(false);
        SBServerManager sbd = ProcessRegistry.get(alias);
        Dequeuer d = sbd.getDequeuer(streamName);
        Schema schema = d.getSchema();
        Parse row = rows;

        logger.debug("about to unordered dequeue {} tuples", expectedRows);
        logger.info("unordered dequeue, header: {}", Arrays.asList(bindingFieldNames));
                
        TupleComparator comparator = new FieldBasedTupleComparator(FieldBasedTupleComparator.DEFAULT_COMPARATOR, bindingFieldNames); 
        SchemaFieldColumnMapper mapper = new SchemaFieldColumnMapper(d.getSchema(), bindingFieldNames);
        List<String> tupleRows = getMappedCSVRowsFromTable(row, mapper);
        
        Expecter e = new Expecter(d, comparator);
        boolean foundError = false;
        
        try {
			e.expectUnordered(Expecter.DequeueSetting.ALLOW_EXTRA_TUPLES, CSVTupleMaker.MAKER, tupleRows);
		} catch (Expecter.ComparisonFailure cf) {
			foundError = showUnorderedDequeueErrors(rows, schema, comparator, mapper, cf);
		} catch(Expecter.ExpectedException ee) {
			foundError = showUnorderedDequeueErrors(rows, schema, comparator, mapper, ee);			
		}
        
        //
        // No error, so we'll report that everything was found
        //
		if(!foundError)
			markAllCorrect(rows);
    }

    private List<Tuple> getTuplesFromTable(Schema schema, Parse row, SchemaFieldColumnMapper mapper) throws StreamBaseException {
        List<String> tupleRows = getMappedCSVRowsFromTable(row, mapper);
        List<Tuple> tuples = new ArrayList<Tuple>();
        
        for(String csv : tupleRows) {
        	Tuple t = CSVTupleMaker.MAKER.createTuple(schema, csv);

        	logger.debug("tuple: {}", t.toString(true));
        	tuples.add(t);
        }
    	
        return tuples;
    }
    
    private List<String> getMappedCSVRowsFromTable(Parse row, SchemaFieldColumnMapper mapper) throws StreamBaseException {
        List<String> tupleRows = new ArrayList<String>();

        while((row=row.more) != null) {
            Parse cell = row.parts;
            String [] csvRow = new String[bindingFieldNames.length];
            
        	for (int column = 0; column < bindings.length; column++, cell = cell.more) {
        		csvRow[column] = cell.text();
        	}

        	logger.info("csv row: {}", Arrays.asList(csvRow));
        	
        	String mappedRow = mapper.mapCSV(csvRow);
        	
        	tupleRows.add(mappedRow);
        	
        	logger.info("mapped csv row {}", mappedRow);
        }
		return tupleRows;
	}

	private boolean showUnorderedDequeueErrors(Parse rows, Schema schema, TupleComparator comparator, SchemaFieldColumnMapper mapper,
			Expecter.ExpectedErrorInfo eei) throws StreamBaseException {
		Parse row;
		boolean foundError;
		foundError = true;
		
		row = rows;
		while((row=row.more) != null) {
		    Parse cell = row.parts;
		    String [] csvRow = new String[bindingFieldNames.length];
		    
			for (int column = 0; column < bindings.length; column++, cell = cell.more) {
				csvRow[column] = cell.text();
			}

			String mappedRow = mapper.mapCSV(csvRow);
			Tuple t = CSVTupleMaker.MAKER.createTuple(schema, mappedRow);
			boolean correct = false;
			
			// was this tuple found?
			for(Tuple found : eei.getFoundTuples()) {
				if(comparator.compare(t, found)) {
					correct = true;
					cell = row.parts;
					for (int column = 0; column < bindings.length; column++, cell = cell.more) {
		        		right(cell);
					}
				}
			}
			
			if(!correct) {
				cell = row.parts;
				for (int column = 0; column < bindings.length; column++, cell = cell.more) {
					wrong(cell);
				}					
			}
		}
		
		for(Tuple t : eei.getUnexpectedTuples()) {
			addUnexpectedRow(rows.last(), t);
		}
		return foundError;
	}
    
	private boolean showNotInDequeueErrors(Parse rows, Schema schema, TupleComparator comparator, SchemaFieldColumnMapper mapper,
			List<Tuple> found, List<Tuple> unmatched) throws StreamBaseException {
		Parse row;
		boolean foundError;
		foundError = true;
		
		row = rows;
		while((row=row.more) != null) {
		    Parse cell = row.parts;
		    String [] csvRow = new String[bindingFieldNames.length];
		    
			for (int column = 0; column < bindings.length; column++, cell = cell.more) {
				csvRow[column] = cell.text();
			}

			String mappedRow = mapper.mapCSV(csvRow);
			Tuple t = CSVTupleMaker.MAKER.createTuple(schema, mappedRow);
			boolean correct = true;
			
			// was this tuple found?
			for(Tuple f : found) {
				if(comparator.compare(t, f)) {
					correct = false;
					cell = row.parts;
					for (int column = 0; column < bindings.length; column++, cell = cell.more) {
		        		wrong(cell);
					}
				}
			}
			
			if(correct) {
				cell = row.parts;
				for (int column = 0; column < bindings.length; column++, cell = cell.more) {
					right(cell);
				}					
			}
		}
		
		for(Tuple t : unmatched) {
			addUnexpectedRow(rows.last(), t);
		}
		return foundError;
	}

	
	
    private void markAllCorrect(Parse p) {
    	if(p == null)
    		return;
    	
    	right(p);
    	
    	while((p = p.more) != null) {
    		right(p);
        	for (Parse cell=p.parts; cell != null && cell.more != null; cell = cell.more) {
        		right(cell);
        		markAllCorrect(cell.parts);
        	}    		
    	}
    }
    
    private long setTimeout(long timeout) {
        this.timeout = timeout;
        return timeout;
    }

    private long getTimeout(boolean expectEmpty) {
        if (timeout == -1) {
            return expectEmpty ? 500 : 3000;
        }
        return timeout;
    }
}
