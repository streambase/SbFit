//
// Copyright (c) 2004-2011 StreamBase Systems, Inc. All rights reserved.
//
package com.streambase.sb.adapter.common.csv;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

/**
 * Implements RFC4180-style CSV file reading, with the extension that
 * the delimiter and quote character can be specified.
 *<p>
 * Note that this does currently not implement the optional header
 * line specified in the RFC.
 */
public class CSVTupleReader
{
    private PushbackReader _reader;
    private int _pushbackBufferSize = 1;

    private char _delimiter, _quoteChar;

    private boolean _quoteCharUsed = false;

    private final StringBuilder _buffer = new StringBuilder();
    private final List<String> _fields = new ArrayList<String>();    
    /**
     * Construct an RFC4180 CSV reader.
     */
    public CSVTupleReader(Reader reader, char delimiter, char quoteChar)
    {
        _reader = new PushbackReader(reader, _pushbackBufferSize);
        _delimiter = delimiter;
        _quoteChar = quoteChar;
        _quoteCharUsed = true;
    }
    
    public CSVTupleReader(Reader reader, char delimiter)
    {
        _reader = new PushbackReader(reader, 1);
        _delimiter = delimiter;
    }

    /**
     * Construct an RFC4180 CSV reader with the default comma
     * delimiter and double quote quote character.
     */
    public CSVTupleReader(Reader reader)
    {
        this(reader, ',', '"');
    }

    /**
     * Read a record from the reader.  This reads fields until a
     * record delimiter is encountered.
     */
    public String[] readRecord() throws IOException, RFC4180FormatException {
        return readRecord(false);
    }
    public String[] readRecord(boolean tailMode) throws IOException, RFC4180FormatException {
        int ch;

        // Consume heading CR and NL and look for EOF
        do {
            ch = _reader.read();
        } while (ch == '\r' || ch == '\n');

        // Did we reach EOF without a record?
        if (ch == -1) {
            return null;
        }

        _reader.unread(ch);
        int columnCount=0;
        _fields.clear();
        
        // When in tail mode, it's possible to see a partially-flushed record at the end of the file. Check
        // for this condition before processing the record
        if (tailMode && !haveCompleteRecord()) {
            return null;
        }
        
        // Now actually read the record
        while (true) {
            _buffer.setLength(0);
            _fields.add(readField(_buffer));
            columnCount++;

            // What type of delimiter follows the record?
            ch = _reader.read();
            if (ch == _delimiter)
                // There's another field
                continue;
            else if (ch == -1 || ch == '\r' || ch == '\n')
                // End of record
                break;
            else
                // Could be a bug, or a badly formed quoted field
                throw new RFC4180FormatException("Expected field or record delimiter, got \"" + (char)ch +"\"{0}{1}"+", column: "+columnCount);
        }

        return _fields.toArray(new String[1]);
    }
    
    /**
     * Determines if a complete record (one ending in [\r\n]+) is currently available 
     * 
     * @return true if a complete record is available and false otherwise
     */
    private boolean haveCompleteRecord() throws IOException {

        // Assume we have a complete record. 
        boolean ret = true;
        
        // Need to save the characters we read from the pushback reader so we can push
        // them back in before returning
        Stack<Integer> savedCharacters = new Stack<Integer>();
        boolean inField = false;
        boolean isQuotedField = false;
        
        // Loop until we find a complete record or hit end of file
        while (true) {
            int ch = _reader.read();
            
            // End of file means we don't have a complete record
            if (ch == -1) {
                ret = false;
                break;
            }
            savedCharacters.push(ch);
            if (!inField) {
                if (ch == '\r' || ch == '\n') {
                    break;
                }
                if (ch == _quoteChar) {
                    isQuotedField = true;
                }
                inField = true;
                continue;
            }
            
            if (!isQuotedField) {
                
                // In non-quoted field. If a CR or LF is found, then we have a complete record
                if (ch == '\r' || ch == '\n') {
                    break;
                }
                if (ch == _delimiter) {
                    inField = false;
                }
            }
            
            // In a quoted field
            else if (ch == _quoteChar) {
                
                // Read the next character to see if the quote character is being quoted 
                ch = _reader.read();
                
                // End of file means we don't have a complete record
                if (ch == -1) {
                    ret = false;
                    break;
                }
                savedCharacters.push(ch);
                
                // If the character following the quote character is a non-quote character,
                // then we're done procesing the quoted field. 
                if (ch != _quoteChar) {
                    inField = false;
                    isQuotedField = false;
                    
                    // If a CR or LF is found, then we have a complete record
                    if (ch == '\r' || ch == '\n') {
                        break;
                    }
                }
            }
        }
        
        // Restore the contents of the pushback reader to its value when we entered this method. In
        // doing so, expand the pushback reader's buffer, if necessary
        int savedSize = savedCharacters.size();
        if (savedSize > _pushbackBufferSize) {
            _pushbackBufferSize = savedSize * 2;    
            _reader = new PushbackReader(_reader, _pushbackBufferSize);
        }
        while (savedCharacters.size() > 0) {
            _reader.unread(savedCharacters.pop());
        }
        
        return ret;
    }

    /**
     * Read the next field from the reader.  The reader should be
     * positioned at the first character of the field.  When this
     * returns, the next character read from the reader will be the
     * delimiter (either the field delimiter, the record delimiter, or
     * EOF).
     * @param field 
     */
    private String readField(StringBuilder field)
        throws IOException, RFC4180FormatException
    {
        int ch = _reader.read();

        if (_quoteCharUsed && (ch == _quoteChar)) {
            // Quoted field
            readQuotedField(field);
            return field.toString();
        } else {
            // Read until the delimiter, newline, or EOF
            if (ch != -1)
            	_reader.unread(ch);
            readRegularField(field);
            return field.toString().trim();
        }
    }

    /**
     * Read a quoted field.  When this returns, the next character
     * read from the reader will be the delimiter.
     * @param field 
     */
    private void readQuotedField(StringBuilder field)
        throws IOException, RFC4180FormatException
    {
        int ch;

        while (true) {
            ch = _reader.read();
            if (ch == -1) {
                throw new RFC4180FormatException("EOF in quoted field");
            }
            if (ch == _quoteChar) {
                // Might be doubled up, read the next character to
                // find out
                int ch2 = _reader.read();
                if (ch2 == ch) {
                    // Doubled quote character is an escaped quote
                    // character
                    field.append((char)ch);
                } else {
                    // We've reached the end of the field
                    if (ch2 != -1)
                        _reader.unread(ch2);
                    return;
                }
            } else {
                // Regular character
                field.append((char)ch);
            }
        }
    }

    /**
     * Read a regular, non-quoted field.  When this returns, the next
     * character read from the reader will be the delimiter.
     * @param field 
     */
    private void readRegularField(StringBuilder field)
        throws IOException, RFC4180FormatException
    {
        int ch;

        while (true) {
            ch = _reader.read();
            if (ch == _delimiter || ch == '\r' || ch == '\n' || ch == -1) {
                if (ch != -1)
                    _reader.unread(ch);
                return;
            }
            field.append((char)ch);
        }
    }
    
    private static class TestCase {
        String initialString;
        String expectedResult;
        public TestCase(String initialString, String expectedResult) {
            this.initialString = initialString;
            this.expectedResult = expectedResult;
        }
    }

    // Unit tests for the readRecord() method with tailMode=true, eventually to be rolled into an 
    // automated test
    public static void main(String[] args) {
        
        System.out.println("");
        
        TestCase[] testCases = new TestCase[] {
                new TestCase("", null),
                new TestCase("\n", null),
                new TestCase(",", null),
                new TestCase("1.05", null),
                new TestCase("1.05,", null),
                new TestCase("1.05,This is a string,1", null),
                new TestCase("1.05,\"This is a string\",1", null),
                new TestCase("1.05,\"\nThis is a string\",1", null),
                new TestCase("1.05,\"This is \"\"the\"\" string\",1", null),
                new TestCase(",\n", "[, ]"),
                new TestCase("1.05\n", "[1.05]"),
                new TestCase("1.05,\n", "[1.05, ]"),
                new TestCase("1.05,This is a string,1\n", "[1.05, This is a string, 1]"),
                new TestCase("1.05,\"This is a string\",1\n", "[1.05, This is a string, 1]"),
                new TestCase("1.05,\"\nThis is a string\",1\n", "[1.05, \nThis is a string, 1]"),
                new TestCase("1.05,\"This is \"\"the\"\" string\",1\n", "[1.05, This is \"the\" string, 1]"),
        };
        
        for (TestCase tc : testCases) {
            try {
                String initialString = tc.initialString;
                String expectedResult = tc.expectedResult;
                CSVTupleReader csvTupleReader = new CSVTupleReader(new InputStreamReader(new ByteArrayInputStream(initialString.getBytes())));
                String[] record = csvTupleReader.readRecord(true);
                initialString = initialString.replaceAll("\\n", "\\\\n");
                expectedResult = expectedResult == null ? null : expectedResult.replaceAll("\\n", "\\\\n");  
                String actualResult = record == null ? null : Arrays.asList(record).toString().replaceAll("\\n", "\\\\n");  
                System.out.println(String.format("Initial string: '%s' -> Result: '%s'", initialString, actualResult));
                int nullCount = (expectedResult == null ? 1 : 0) + (actualResult == null ? 1 : 0);
                if (nullCount == 1 || (nullCount == 0 && !expectedResult.equals(actualResult))) {
                    throw new Exception(String.format("Initial string: '%s', Expected result: '%s', Actual result: '%s'", initialString, expectedResult, actualResult)); 
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
}
