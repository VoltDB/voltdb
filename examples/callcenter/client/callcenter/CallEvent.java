/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package callcenter;

import java.util.Date;

/**
 * Represents a begin or end call event.
 *
 * Begin call has a null endTS.
 * Start call has a null startTS.
 *
 * Immutable.
 *
 */
public class CallEvent {
    final long callId;
    final int agentId;
    final long phoneNo;
    final Date startTS;
    final Date endTS;

    CallEvent(long callId, int agentId, long phoneNo, Date startTS, Date endTS) {
        this.callId = callId;
        this.agentId = agentId;
        this.phoneNo = phoneNo;
        this.startTS = startTS;
        this.endTS = endTS;
    }

    public String phoneNoStr() {
        return String.format("+1 (%d) %d-%04d",
                phoneNo / 10000000, (phoneNo / 10000) % 1000, phoneNo % 10000);
    }
}
