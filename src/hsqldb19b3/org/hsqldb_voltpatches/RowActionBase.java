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

/**
 * Represents a single insert / delete / rollback / commit action on a row.
 *
 * type = type of action
 * actionTimestamp = timestamp of end of SQL action; 0 if action not complete
 * commitTimestamp = timestamp of commit or rollback; 0 if not committed/rolledback
 * rolledBack = flag for rolled back actions
 * next = next action in linked list;
 *
 * timestamps are not in any order
 *
 * @author Fred Toussi (fredt@users dot sourceforge dot net)
 * @version 2.3.2

 * @since 2.0.0
 */
public class RowActionBase {

    public static final byte ACTION_NONE          = 0;
    public static final byte ACTION_INSERT        = 1;
    public static final byte ACTION_DELETE        = 2;
    public static final byte ACTION_DELETE_FINAL  = 3;
    public static final byte ACTION_INSERT_DELETE = 4;
    public static final byte ACTION_REF           = 5;
    public static final byte ACTION_CHECK         = 6;
    public static final byte ACTION_DEBUG         = 7;

    //
    RowActionBase            next;
    Session                  session;
    long                     actionTimestamp;
    long                     commitTimestamp;
    byte                     type;
    boolean                  deleteComplete;
    boolean                  rolledback;
    boolean                  prepared;
    int[]                    changeColumnMap;

    RowActionBase() {}

    /**
     * constructor, used for delete actions only
     */
    RowActionBase(Session session, byte type) {

        this.session    = session;
        this.type       = type;
        actionTimestamp = session.actionTimestamp;
    }

    void setAsAction(RowActionBase action) {

        next            = action.next;
        session         = action.session;
        actionTimestamp = action.actionTimestamp;
        commitTimestamp = action.commitTimestamp;
        type            = action.type;
        deleteComplete  = action.deleteComplete;
        rolledback      = action.rolledback;
        prepared        = action.prepared;
        changeColumnMap = action.changeColumnMap;
    }
}
