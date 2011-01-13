/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.debugstate;

import java.io.Serializable;
import org.voltdb.dtxn.InFlightTxnState;

public class InitiatorContext extends VoltThreadContext implements Serializable, Comparable<InitiatorContext> {
    private static final long serialVersionUID = -3607694863458970146L;

    public int siteId;
    public InFlightTxnState[] inFlightTxns;
    public MailboxHistory mailboxHistory;

    @Override
    public int compareTo(InitiatorContext o) {
        if (o == null) return -1;
        return siteId - o.siteId;
    }
}
