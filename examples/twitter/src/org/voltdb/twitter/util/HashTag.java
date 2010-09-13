/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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
package org.voltdb.twitter.util;

import org.voltdb.VoltTableRow;

public class HashTag implements Comparable<HashTag> {

    private String hashTag;
    private int count;
    private VoltTableRow row;

    public HashTag(String hashTag, int count) {
        this(hashTag, count, null);
    }

    public HashTag(String hashTag, int count, VoltTableRow row) {
        this.hashTag = hashTag;
        this.count = count;
        this.row = row;
    }

    public String getHashTag() {
        return hashTag;
    }

    public int getCount() {
        return count;
    }

    public VoltTableRow getRow() {
        return row;
    }

    @Override
    public int compareTo(HashTag other) {
        if (other.getCount() - count != 0) {
            return other.getCount() - count;
        } else {
            return hashTag.compareTo(other.getHashTag());
        }
    }

    @Override
    public String toString() {
        return hashTag + " " + count;
    }

}
