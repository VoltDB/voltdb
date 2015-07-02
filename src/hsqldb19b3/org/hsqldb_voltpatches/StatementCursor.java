/* Copyright (c) 2001-2011, The HSQL Development Group
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

import org.hsqldb_voltpatches.ParserDQL.CompileContext;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.result.Result;

/**
 * Implementation of Statement for PSM and trigger assignment.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 2.0.1
 */
public class StatementCursor extends StatementQuery {

    public static final StatementCursor[] emptyArray = new StatementCursor[]{};

    StatementCursor(Session session, QueryExpression query,
                    CompileContext compileContext) {
        super(session, query, compileContext);
    }

    Result getResult(Session session) {

        Object[] data    = session.sessionContext.routineArguments;
        Result   current = (Result) data[data.length - 1];
        Result   head    = current;

        while (current != null) {
            if (getCursorName().name.equals(current.getMainString())) {
                current.navigator.release();

                if (head == current) {
                    head = current.getChainedResult();
                }
            }

            if (current.getChainedResult() == null) {
                break;
            }

            current = current.getChainedResult();
        }

        data[data.length - 1] = head;

        Result result = queryExpression.getResult(session, 0);

        result.setStatement(this);

        if (result.isError()) {
            return result;
        }

        result.setMainString(getCursorName().name);

        if (head == null) {
            data[data.length - 1] = result;
        } else {
            ((Result) data[data.length - 1]).addChainedResult(result);
        }

        return Result.updateZeroResult;
    }

    /**
     * always readonly
     */
    void collectTableNamesForWrite(OrderedHashSet set) {}

}
