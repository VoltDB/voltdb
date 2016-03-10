/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package metrocard;

import java.util.*;

/** Parse comma-separated values (CSV), a common Windows file format.
 * Sample input: "LU",86.25,"11/4/1998","2:19PM",+4.0625
 * <p>
 * Inner logic adapted from a C++ original that was
 * Copyright (C) 1999 Lucent Technologies
 * Excerpted from 'The Practice of Programming'
 * by Brian W. Kernighan and Rob Pike.
 * <p>
 * Included by permission of the http://tpop.awl.com/ web site,
 * which says:
 * "You may use this code for any purpose, as long as you leave
 * the copyright notice and book citation attached." I have done so.
 * @author Brian W. Kernighan and Rob Pike (C++ original)
 * @author Ian F. Darwin (translation into Java and removal of I/O)
 * @author Ben Ballard (updates, removal of dependencies)
 */
public class CsvLineParser {

    public static final char DEFAULT_SEP = ',';
    private static final boolean debug = false;

    /** Construct a CSV parser, with the default separator (`,'). */
    public CsvLineParser() {
        this(DEFAULT_SEP);
    }

    /** Construct a CSV parser with a given separator. Must be
     * exactly the string that is the separator, not a list of
     * separator characters!
     */
    public CsvLineParser(char sep) {
        fieldSep = sep;
    }

    /** The fields in the current String */
    protected ArrayList<String> list = new ArrayList<String>();

    /** the separator char for this parser */
    protected char fieldSep;

    /** parse: break the input String into fields
     * @return java.util.Iterator containing each field
     * from the original as a String, in order.
     */
    public Iterator parse(String line) {
        StringBuffer field = new StringBuffer();
        list.clear();           // discard previous, if any
        int p = 0;

        if (line.length() == 0) {
            list.add(line);
            return list.iterator();
        }

        do {
            field.setLength(0);
            if (p < line.length() && line.charAt(p) == '"')
                p = advQuoted(line, field, ++p);    // skip quote
            else
                p = advPlain(line, field, p);
            list.add(field.toString());
            if (debug) System.out.println(field.toString());
            p++;
        } while (p < line.length());

        return list.iterator();
    }

    /** advQuoted: quoted field; return index of next separator */
    protected int advQuoted(String s, StringBuffer field, int i) {
        int j;
        int len= s.length();
        for (j=i; j<len; j++) {
            if (s.charAt(j) == '"' && j+1 < len) {
                if (s.charAt(j+1) == '"') {
                    j++; // skip escape char
                } else if (s.charAt(j+1) == fieldSep) { //next delimeter
                    j++; // skip end quotes
                    break;
                }
            } else if (s.charAt(j) == '"' && j+1 == len) { // end quotes at end of line
                break; //done
            }
            field.append(s.charAt(j));  // regular character.
        }
        return j;
    }

    /** advPlain: unquoted field; return index of next separator */
    protected int advPlain(String s, StringBuffer field, int i) {
        int j;

        j = s.indexOf(fieldSep, i); // look for separator
        if (debug) System.out.println("i = " + i + ", j = " + j);
        if (j == -1) {                  // none found
            field.append(s.substring(i));
            return s.length();
        } else {
            field.append(s.substring(i, j));
            return j;
        }
    }
}
