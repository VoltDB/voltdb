/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.hsqldb_voltpatches;

//VoltDB extension to support TTL
public class TimeToLiveVoltDB {
    public final static String TTL_NAME = "ttl";
    final int ttlValue;
    final String ttlUnit;
    final ColumnSchema ttlColumn;
    final int batchSize;
    final int maxFrequency;
    final String stream;
    public TimeToLiveVoltDB(int value, String unit, ColumnSchema column,
            int batchSize, int maxFrequency, String streamName) {
        ttlValue = value;
        ttlUnit = unit;
        ttlColumn = column;
        this.batchSize = batchSize;
        this.maxFrequency = maxFrequency;
        stream = streamName;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TimeToLiveVoltDB) {
            TimeToLiveVoltDB ttl = (TimeToLiveVoltDB)o;
            return (ttl.ttlValue == ttlValue && ttl.ttlUnit.equalsIgnoreCase(ttlUnit) &&
                    ttl.ttlColumn.getName().equals(ttlColumn.getName()) &&
                    ttl.batchSize == batchSize && ttl.maxFrequency == maxFrequency &&
                    ttl.stream.equals(stream));
        }
        return false;
    }
}