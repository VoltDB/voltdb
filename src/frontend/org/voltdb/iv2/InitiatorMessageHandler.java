/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.iv2;

import java.util.List;

import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.CommandLog;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

/**
 * InitiatorMessageHandler delivers messages to internal Initiator components.
 * Different roles (multi/single-part initiator/replica) should extend this
 * class to provide routing appropriate to the role.
 * IZZY: I think this could merge with Scheduler pretty cleanly.
 */
public interface InitiatorMessageHandler
{
    void updateReplicas(List<Long> replicas);

    void setCommandLog(CommandLog cl);
    void setMailbox(Mailbox mailbox);
        /*
           if (replica):
               if (sp procedure):
                   log it
                   make a SP-task cfg'd with respond-to-remote
                if (mp fragment):
                   log it
                   make a MP-fragtask cfg'd with respond-to-master

            if (master):
                if (sp procedure):
                    log it
                    replicate
                    make a SP-task cfg'd with respond-to-local
                if (mp fragment)
                    log it
                    replicate
                    make a MP-fragtask cfg'd with respond-to-local
                if (sp-response):
                    log it?
                    if replicate.dedupe() says complete:
                       send response to creator
                if (mp fragment response)
                    log it?
                    if replicate.dedupe() says complete:
                       send response to creator (must be MPI)
                if (complete transaction)
                    log it
                    replicate
                    make a complete-transaction task

            if (mpi):
                if (mp procedure):
                    log it
                    make a mp procedure task
                if (mp fragment response):
                    offer to txnstate
                if (every-site):
                    log it
                    send sp procedure to every master
                if (every-site-response):
                    if all-responses-collected? respond to creator

       */

    // deliver a message for handling in normal operation.
    void deliver(VoltMessage message);

    // deliver a repair request to a leader starting a new Term.
    void handleIv2InitiateTaskMessageRepair(List<Long> needsRepair, Iv2InitiateTaskMessage message);
}
