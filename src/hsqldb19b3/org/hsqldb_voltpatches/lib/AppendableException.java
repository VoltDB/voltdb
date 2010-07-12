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


package org.hsqldb_voltpatches.lib;

import java.util.ArrayList;
import java.util.List;

/**
 * Allows additional messages to be appended.
 *
 * It often makes for better (and more efficient) design to add context
 * details to an exception at intermediate points in the thread.
 * This class makes it easy and efficient to catch and rethrow for that purpose.
 */
public class AppendableException extends Exception {

    static final long    serialVersionUID = -1002629580611098803L;
    public static String LS = System.getProperty("line.separator");
    public List          appendages       = null;

    public String getMessage() {

        String message = super.getMessage();

        if (appendages == null) {
            return message;
        }

        StringBuffer sb = new StringBuffer();

        if (message != null) {
            sb.append(message);
        }

        for (int i = 0; i < appendages.size(); i++) {
            if (sb.length() > 0) {
                sb.append(LS);
            }

            sb.append(appendages.get(i));
        }

        return sb.toString();
    }

    public void appendMessage(String s) {

        if (appendages == null) {
            appendages = new ArrayList();
        }

        appendages.add(s);
    }

    public AppendableException() {}

    public AppendableException(String s) {
        super(s);
    }

    public AppendableException(Throwable cause) {
        super(cause);
    }

    public AppendableException(String string, Throwable cause) {
         super(string, cause);
    }
}
