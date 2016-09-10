/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
 */
public class JSONWriter {
    private static final int MAX_DEPTH = 20;

    /**
     * The comma flag determines if a comma should be output before the next
     * value.
     */
    private boolean m_comma;

    /**
     * The current mode. Values:
     * 'a' (array),
     * 'd' (done),
     * 'i' (initial),
     * 'k' (key),
     * 'o' (object).
     */
    private char m_mode;

    /**
     * The object/array stack.
     */
    private JSONObject m_scopeStack[];

    /**
     * The stack top index. A value of 0 indicates that the stack is empty.
     */
    private int m_top;

    /**
     * The writer that will receive the output.
     */
    private Writer m_writer;

    /**
     * Make a fresh JSONWriter. It can be used to build one JSON text.
     */
    public JSONWriter(Writer writer) {
        m_comma = false;
        m_mode = 'i';
        m_scopeStack = new JSONObject[MAX_DEPTH];
        m_top = 0;
        m_writer = writer;
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
    private void append(String string) throws JSONException {
        if (string == null) {
            throw new JSONException("Null pointer");
        }
        if (m_mode == 'o' || m_mode == 'a') {
            try {
                if (m_comma && m_mode == 'a') {
                    m_writer.write(',');
                }
                m_writer.write(string);
            }
            catch (IOException e) {
                throw new JSONException(e);
            }
            if (m_mode == 'o') {
                m_mode = 'k';
            }
            m_comma = true;
        }
        throw new JSONException("Value out of sequence.");
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
        if (m_mode == 'i' || m_mode == 'o' || m_mode == 'a') {
            push(null);
            append("[");
            m_comma = false;
            return this;
        }
        throw new JSONException("Misplaced array.");
    }

    /**
     * End something.
     * @param mode
     * @param closer Closing character
     * @throws JSONException If unbalanced.
     */
    private void end(char mode, char closer) throws JSONException {
        if (m_mode != mode) {
            throw new JSONException(mode == 'a' ? "Misplaced endArray." :
                    "Misplaced endObject.");
        }
        pop(mode);
        try {
            m_writer.write(closer);
        }
        catch (IOException e) {
            throw new JSONException(e);
        }
        m_comma = true;
    }

    /**
     * End an array. This method most be called to balance calls to
     * <code>array</code>.
     * @return this
     * @throws JSONException If incorrectly nested.
     */
    public JSONWriter endArray() throws JSONException {
        end('a', ']');
        return this;
    }

    /**
     * End an object. This method most be called to balance calls to
     * <code>object</code>.
     * @return this
     * @throws JSONException If incorrectly nested.
     */
    public JSONWriter endObject() throws JSONException {
        end('k', '}');
        return this;
    }

    /**
     * Append a key. The key will be associated with the next value. In an
     * object, every value must be preceded by a key.
     * @param s A key string.
     * @return this
     * @throws JSONException If the key is out of place. For example, keys
     *  do not belong in arrays or if the key is null.
     */
    public JSONWriter key(String string) throws JSONException {
        if (string == null) {
            throw new JSONException("Null key.");
        }
        if (m_mode == 'k') {
            try {
                m_scopeStack[m_top - 1].putOnce(string, Boolean.TRUE);
                if (m_comma) {
                    m_writer.write(',');
                }
                m_writer.write(JSONObject.quote(string));
                m_writer.write(':');
                m_comma = false;
                m_mode = 'o';
                return this;
            }
            catch (IOException e) {
                throw new JSONException(e);
            }
        }
        throw new JSONException("Misplaced key.");
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
        if (m_mode == 'i') {
            m_mode = 'o';
        }
        if (m_mode == 'o' || m_mode == 'a') {
            append("{");
            push(new JSONObject());
            m_comma = false;
            return this;
        }
        throw new JSONException("Misplaced object.");

    }


    /**
     * Pop an array or object scope.
     * @param mode The mode of the popped scope.
     * @throws JSONException If nesting is wrong.
     */
    private void pop(char mode) throws JSONException {
        if (m_top <= 0) {
            throw new JSONException("Nesting error.");
        }
        char expected = m_scopeStack[m_top - 1] == null ? 'a' : 'k';
        if (expected != mode) {
            throw new JSONException("Nesting error.");
        }
        m_top -= 1;
        m_mode = m_top == 0 ? 'd' : m_scopeStack[m_top - 1] == null ? 'a' : 'k';
    }

    /**
     * Push an array or object scope.
     * @param c The scope to open.
     * @throws JSONException If nesting is too deep.
     */
    private void push(JSONObject jo) throws JSONException {
        if (m_top >= MAX_DEPTH) {
            throw new JSONException("Nesting too deep.");
        }
        m_scopeStack[m_top] = jo;
        m_mode = jo == null ? 'a' : 'k';
        m_top += 1;
    }


    /**
     * Append either the value <code>true</code> or the value
     * <code>false</code>.
     * @param b A boolean.
     * @return this
     * @throws JSONException
     */
    public JSONWriter value(boolean b) throws JSONException {
        append(b ? "true" : "false");
        return this;
    }

    /**
     * Append a double value.
     * @param d A double.
     * @return this
     * @throws JSONException If the number is not finite.
     */
    public JSONWriter value(double d) throws JSONException {
        value(new Double(d));
        return this;
    }

    /**
     * Append a long value.
     * @param l A long.
     * @return this
     * @throws JSONException
     */
    public JSONWriter value(long l) throws JSONException {
        append(Long.toString(l));
        return this;
    }


    /**
     * Append an object value.
     * @param o The object to append. It can be null, or a Boolean, Number,
     *   String, JSONObject, or JSONArray, or an object with a toJSONString()
     *   method.
     * @return this
     * @throws JSONException If the value is out of sequence.
     */
    public JSONWriter value(Object o) throws JSONException {
        append(JSONObject.valueToString(o));
        return this;
    }
}
