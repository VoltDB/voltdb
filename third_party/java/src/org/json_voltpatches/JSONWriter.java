/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.json_voltpatches;

import java.io.IOException;
import java.io.Writer;
import java.util.HashSet;

/*
Copyright (c) 2006 JSON.org

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

The Software shall be used for Good, not Evil.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

/**
 * JSONWriter provides a quick and convenient way of producing JSON text.
 * The texts produced strictly conform to JSON syntax rules. No whitespace is
 * added, so the results are ready for transmission or storage. Each instance of
 * JSONWriter can produce one JSON text.
 * <p>
 * A JSONWriter instance provides a <code>value</code> method for appending
 * values to the
 * text, and a <code>key</code>
 * method for adding keys before values in objects. There are <code>array</code>
 * and <code>endArray</code> methods that make and bound array values, and
 * <code>object</code> and <code>endObject</code> methods which make and bound
 * object values. All of these methods return the JSONWriter instance,
 * permitting a cascade style. For example, <pre>
 * new JSONWriter(myWriter)
 *     .object()
 *         .key("JSON")
 *         .value("Hello, World!")
 *     .endObject();</pre> which writes <pre>
 * {"JSON":"Hello, World!"}</pre>
 * <p>
 * The first method called must be <code>array</code> or <code>object</code>.
 * There are no methods for adding commas or colons. JSONWriter adds them for
 * you. Objects and arrays can be nested up to 20 levels deep.
 * <p>
 * This can sometimes be easier than using a JSONObject to build a string.
 * @author JSON.org
 * @version 2010-03-11
 *
 * This code has been SIGNIFICANTLY refactored in VoltDB for readability
 * and performance, but behaves largely as the original code did with a few
 * convenience calls layered on.
 */
public class JSONWriter {
    private static final int MAX_DEPTH = 20;

    /**
     * The comma flag determines if a comma should be output before the next
     * value. It controls an internally managed sub-state of some of the
     * finite machine states (states 'a' and 'k').
     */
    private boolean m_expectingComma;

    /**
     * The current mode.
     * These are the allowed states in a finite state machine representing the
     * the JSONWriter AND its most deeply nested active scope, if it has one.
     * Values:
     * 'a' (array), an active array scope is expecting values or termination
     * 'd' (done), no active scope, expects no further input to the writer
     * 'i' (initial), no active scope, expects an object or array to start one.
     * 'k' (key), an active object scope is expecting a key or termination.
     * 'o' (object), an active object scope is expecting a value (after a key).
     */
    private char m_mode;

    /**
     * Since
     *     private final HashSet<String> m_scopeStack[] = new HashSet<String>[MAX_DEPTH];
     * gives
     *     error: generic array creation
     * define a trivially compatible class to use in place of the generic for
     * array and array member initialization.
     */
    private static class HashSetOfString extends HashSet<String> {
        private static final long serialVersionUID = 1L; // don't care
    };

    /**
     * The object/array scope stack.
     */
    private final HashSet<String> m_scopeStack[] = new HashSetOfString[MAX_DEPTH];

    /**
     * The stack top index. A value of -1 indicates that the stack is empty.
     * A value of 0 indicates the outermost active scope, 1 indicates an active
     * scope nested within the outermost, and so on.
     */
    private int m_top;

    /**
     * The writer that will receive the output.
     */
    private final Writer m_writer;

    /**
     * Make a fresh JSONWriter. It can be used to build one JSON text.
     * It expects an initial call to <code>object</code> or <code>array</code>.
     */
    public JSONWriter(Writer writer) {
        m_writer = writer;
        m_top = -1;
        m_mode = 'i';
        m_expectingComma = false;
    }

    /** An accessor */
    protected Writer getWriter() { return m_writer; }

    /** An abstract test accessor for m_mode */
    protected boolean isDone() { return m_mode == 'd'; }

    /**
     * Append a value, already validated, formatted and/or quoted as needed.
     * @param s A string value.
     * @throws JSONException If the value is out of sequence.
     */
    private void appendValue(String string) throws JSONException {
        assert string != null;
        if (m_mode == 'k' || m_mode == 'i' || m_mode == 'd') {
            throw new JSONException("Value out of sequence.");
        }
        try {
            if (m_mode == 'a') {
                if (m_expectingComma) {
                    m_writer.write(',');
                }
            }
            else if (m_mode == 'o') {
                m_mode = 'k';
            }
            m_writer.write(string);
        }
        catch (IOException e) {
            throw new JSONException(e);
        }
        m_expectingComma = true;
    }

    private static enum ScopeOptions {
        ArrayWithComma(",[") {
            @Override
            HashSetOfString createKeyTracker() { return null; }
        },
        ArrayWithoutComma("[") {
            @Override
            HashSetOfString createKeyTracker() { return null; }
        },
        ObjectWithComma(",{"),
        ObjectWithoutComma("{"),
       ;

       final String m_prefix;

        ScopeOptions(String prefix) {
            m_prefix = prefix;
        }

        HashSetOfString createKeyTracker() {
            return new HashSetOfString();
        }
    }

    /**
     * Push an array or object scope.
     * @param options settings to control prefix output and
     *        optional key tracking for object vs. array scopes.
     * @throws JSONException If nesting is too deep.
     */
    private void push(ScopeOptions options) throws JSONException {
        m_top += 1;
        if (m_top >= MAX_DEPTH) {
            throw new JSONException("Nesting too deep.");
        }
        try {
            m_writer.write(options.m_prefix);
        }
        catch (IOException e) {
            throw new JSONException(e);
        }

        HashSetOfString keyTracker = options.createKeyTracker();
        m_scopeStack[m_top] = keyTracker;
        m_mode = (keyTracker == null) ? 'a' : 'k';
        m_expectingComma = false;
    }

    /**
     * Pop an array or object scope.
     * @param closer Closing character
     * @throws JSONException If nesting is wrong.
     */
    private void pop(char closer) throws JSONException {
        if (m_top <= -1) {
            throw new JSONException("Nesting error.");
        }
        try {
            m_writer.write(closer);
        }
        catch (IOException e) {
            throw new JSONException(e);
        }
        --m_top;
        m_mode = (m_top == -1) ?
                'd' :
                (m_scopeStack[m_top] == null) ? 'a' : 'k';
        m_expectingComma = true;
    }

    /**
     * Begin appending a new array. All values until the balancing
     * <code>endArray</code> will be appended to this array. The
     * <code>endArray</code> method must be called to mark the array's end.
     * @return this
     * @throws JSONException If the nesting is too deep, or if the object is
     * started in the wrong place (for example as a key or after the end of the
     * outermost array or object).
     */
    public JSONWriter array() throws JSONException {
        if (m_mode == 'k' || m_mode == 'd') {
            throw new JSONException("Misplaced array.");
        }
        push(m_expectingComma ?
                ScopeOptions.ArrayWithComma :
                ScopeOptions.ArrayWithoutComma);
        return this;
    }

    /**
     * End an array. This method must be called to balance calls to
     * <code>array()</code>.
     * @return this
     * @throws JSONException If incorrectly nested.
     */
    public JSONWriter endArray() throws JSONException {
        if (m_mode != 'a') {
            throw new JSONException("Misplaced endArray.");
        }
        pop(']');
        return this;
    }

    /**
     * Begin appending a new object. All keys and values until the balancing
     * <code>endObject</code> will be appended to this object. The
     * <code>endObject</code> method must be called to mark the object's end.
     * @return this
     * @throws JSONException If the nesting is too deep, or if the object is
     * started in the wrong place (for example as a key or after the end of the
     * outermost array or object).
     */
    public JSONWriter object() throws JSONException {
        if (m_mode == 'k' || m_mode == 'd') {
            throw new JSONException("Misplaced object.");
        }
        push(m_expectingComma ?
                ScopeOptions.ObjectWithComma :
                ScopeOptions.ObjectWithoutComma);
        return this;
    }

    /**
     * End an object. This method must be called to balance calls to
     * <code>object()</code>.
     * @return this
     * @throws JSONException If incorrectly nested.
     */
    public JSONWriter endObject() throws JSONException {
        if (m_mode != 'k') {
            throw new JSONException("Misplaced endObject.");
        }
        pop('}');
        return this;
    }

    /**
     * Append a key. The key will be associated with the next value. In an
     * object, every value must be preceded by a key.
     * @param string A key string.
     * @return this
     * @throws JSONException If the key is null or the key is out of place.
     * For example, keys do not belong in arrays.
     */
    public JSONWriter key(String string) throws JSONException {
        if (string == null) {
            throw new JSONException("Null key.");
        }
        if (m_mode != 'k') {
            throw new JSONException("Misplaced key.");
        }

        // Throw if the key has already been seen in this scope.
        if ( ! m_scopeStack[m_top].add(string)) {
            throw new JSONException("Duplicate key \"" + string + "\"");
        }

        try {
            if (m_expectingComma) {
                m_writer.write(',');
            }
            m_writer.write(JSONObject.quote(string));
            m_writer.write(':');
        }
        catch (IOException e) {
            throw new JSONException(e);
        }
        m_mode = 'o';
        m_expectingComma = false;
        return this;
    }

    /**
     * Append either the value <code>true</code> or the value
     * <code>false</code>.
     * @param b A boolean.
     * @return this
     * @throws JSONException if the value is out of sequence or
     * if the writer throws an IOException
     */
    public JSONWriter value(boolean aValue) throws JSONException {
        appendValue(aValue ? "true" : "false");
        return this;
    }

    /**
     * Append a double value.
     * @param d A double.
     * @return this
     * @throws JSONException If the number is not finite
     * or if the value is out of sequence or
     * if the writer throws an IOException
     */
    public JSONWriter value(double aValue) throws JSONException {
        appendValue(JSONObject.numberToString(aValue));
        return this;
    }

    /**
     * Append a long value.
     * @param l A long.
     * @return this
     * @throws JSONException if the value is out of sequence or
     * if the writer throws an IOException
     */
    public JSONWriter value(long aValue) throws JSONException {
        appendValue(Long.toString(aValue));
        return this;
    }

    /**
     * Append an object value.
     * @param o The object to append. It can be null, or a Boolean, Number,
     *   String, JSONObject, or JSONArray, or an implementation of JSONString.
     * @return this
     * @throws JSONException if the value is out of sequence or if a
     * toJSONString method throws an Exception or
     * if the writer throws an IOException
     */
    public JSONWriter value(Object aValue) throws JSONException {
        appendValue(JSONObject.valueToString(aValue));
        return this;
    }

    /**
     * Append an object value derived from a custom JSONString implementation.
     * This works identically to <code>value(Object o)</code> called on a
     * on object whose actual type implements JSONString. This method is
     * preferable because it by-passes the run-time type checking of the more
     * general method.
     * @param jss The JSONString object to append.
     * It can be null or implement JSONString.
     * @return this
     * @throws JSONException if the value is out of sequence or if a
     * toJSONString method throws an Exception or
     * if the writer throws an IOException
     */
    public JSONWriter value(JSONString jss) throws JSONException {
        if (jss == null) {
            valueNull();
            return this;
        }

        try {
            String asString = jss.toJSONString();
            if (asString == null) {
                throw new JSONException("Unexpected null from toJSONString");
            }
            appendValue(asString);
            return this;
        }
        catch (Exception e) {
            throw new JSONException(e);
        }
    }

    /**
     * Append a null value
     * @return this
     * @throws JSONException if the value is out of sequence or
     * if the writer throws an IOException
     */
    public JSONWriter valueNull() throws JSONException {
        appendValue("null");
        return this;
    }

    /**
     * Append an array value based on a custom JSONString implementation.
     * @param jss The JSONString array or container to append.
     * Its elements can be null or implement JSONString.
     * @return this
     * @throws JSONException if the value is out of sequence or if a
     * toJSONString method throws an Exception or
     * if the writer throws an IOException
     */
    public JSONWriter array(Iterable<? extends JSONString> iter) throws JSONException {
        array();
        for (JSONString element : iter) {
            value(element);
        }
        endArray();
        return this;
    }

    /**
     * Write a JSON key-value pair in one optimized step that assumes that
     * the key is a symbol composed of normal characters requiring no escaping
     * and asserts that keys are non-null and unique within an object ONLY if
     * asserts are enabled. This method is most suitable in the common case
     * where the caller is making a hard-coded series of calls with the same
     * hard-coded strings for keys. Any sequencing errors can be detected
     * in debug runs with asserts enabled.
     * @param aKey
     * @param aValue
     * @return this
     * @throws JSONException
     */
    public JSONWriter keySymbolValuePair(String aKey, String aValue)
            throws JSONException {
        assert(aKey != null);
        assert(m_mode == 'k');
        // The key should not have already been seen in this scope.
        assert(m_scopeStack[m_top].add(aKey));

        try {
            m_writer.write(m_expectingComma ? ",\"" : "\"");
            m_writer.write(aKey);
	        if (aValue == null) {
                m_writer.write("\":null");
	        }
	        else {
                m_writer.write("\":\"");
                m_writer.write(JSONObject.quotable(aValue));
                m_writer.write('"');
	        }
        }
        catch (IOException e) {
            throw new JSONException(e);
        }
        m_expectingComma = true;
        return this;
    }

    /**
     * Write a JSON key-value pair in one optimized step that assumes that
     * the key is a symbol composed of normal characters requiring no escaping
     * and asserts that keys are non-null and unique within an object ONLY if
     * asserts are enabled. This method is most suitable in the common case
     * where the caller is making a hard-coded series of calls with the same
     * hard-coded strings for keys. Any sequencing errors can be detected
     * in debug runs with asserts enabled.
     * @param aKey
     * @param aValue
     * @return this
     * @throws JSONException
     */
    public JSONWriter keySymbolValuePair(String aKey, long aValue)
            throws JSONException {
        assert(aKey != null);
        assert(m_mode == 'k');
        // The key should not have already been seen in this scope.
        assert(m_scopeStack[m_top].add(aKey));

        try {
            m_writer.write(m_expectingComma ? ",\"" : "\"");
            m_writer.write(aKey);
            m_writer.write("\":");
            m_writer.write(Long.toString(aValue));
        }
        catch (IOException e) {
            throw new JSONException(e);
        }
        m_expectingComma = true;
        return this;
    }

    /**
     * Write a JSON key-value pair in one optimized step that assumes that
     * the key is a symbol composed of normal characters requiring no escaping
     * and asserts that keys are non-null and unique within an object ONLY if
     * asserts are enabled. This method is most suitable in the common case
     * where the caller is making a hard-coded series of calls with the same
     * hard-coded strings for keys. Any sequencing errors can be detected
     * in debug runs with asserts enabled.
     * @param aKey
     * @param aValue
     * @return this
     * @throws JSONException
     */
    public JSONWriter keySymbolValuePair(String aKey, boolean aValue)
            throws JSONException {
        assert(aKey != null);
        assert(m_mode == 'k');
        // The key should not have already been seen in this scope.
        assert(m_scopeStack[m_top].add(aKey));

        try {
            m_writer.write(m_expectingComma ? ",\"" : "\"");
            m_writer.write(aKey);
            m_writer.write(aValue ? "\":true" : "\":false");
        }
        catch (IOException e) {
            throw new JSONException(e);
        }
        m_expectingComma = true;
        return this;
    }

}
