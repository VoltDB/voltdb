/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.sysprocs;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.client.ClientResponse;

public class LogNote  extends VoltNTSystemProcedure {
    private final static VoltLogger log = new VoltLogger("NOTE");

    public static class LogNoteOnHost extends VoltNTSystemProcedure {
        public long run(String username, String remote, String message) {
            log.infoFmt("From %s at %s: %s", username, remote, message);
            return 0;
        }
    }


    public long run(String message) throws InterruptedException, ExecutionException {
        String username = getUsername();
        String remote = null;
        InetSocketAddress remoteAddr = getRemoteAddress();
        if (remoteAddr != null) {
            remote = remoteAddr.getHostString();
        }
        Map<Integer, ClientResponse> result;
        result = callNTProcedureOnAllHosts("@LogNoteOnHost", username == null ? "unknown" : username,
                remote == null ? "unknown" : remote, message).get();
        String err = checkResult(result);
        if (err != null) {
            log.warn(err);
        }
        return 0;
    }

    // Handles the result from execution of subordinate sysprocs on
    // all live hosts.
    private String checkResult(Map<Integer, ClientResponse> results) {
        StringBuilder sb = null;

        for (Entry<Integer, ClientResponse> e : results.entrySet()) {
            // Request failed to execute?
            if (e.getValue().getStatus() != ClientResponse.SUCCESS) {
                if (sb == null) {
                    sb = new StringBuilder("Failed:\n");
                }
                sb.append("Host ").append(e.getKey()).append(" reports: ").append(e.getValue().getStatus()).append("\n");
            }
        }
        return sb == null ? null : sb.toString();
    }
}
