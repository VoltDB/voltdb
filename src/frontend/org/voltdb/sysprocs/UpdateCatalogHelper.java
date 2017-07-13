/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;

/**
 *
 * This system proc is used by UpdateCore related work to write and verify
 * the catalog bytes. The purpose of this proc is to put the catalog write
 * work on the asynchronous thread and reduce blocking time.
 *
 * Modes:
 * CHECK_AND_WRITE  Check the validity of the catalogBytes, and write to a
 * temporary catalog jar file if successful.
 *
 * CLEAN_UP Delete any temporary jar file on this instance.
 *
 */
public class UpdateCatalogHelper extends UpdateApplicationBase {

    public final static HashMap<Integer, String> SupportedJavaVersionMap = new HashMap<>();
    static {
        SupportedJavaVersionMap.put(45, "Java 1.1");
        SupportedJavaVersionMap.put(46, "Java 1.2");
        SupportedJavaVersionMap.put(47, "Java 1.3");
        SupportedJavaVersionMap.put(48, "Java 1.4");
        SupportedJavaVersionMap.put(49, "Java 5");
        SupportedJavaVersionMap.put(50, "Java 6");
        SupportedJavaVersionMap.put(51, "Java 7");
        SupportedJavaVersionMap.put(52, "Java 8");
    }

    // TODO: maybe we can add another option to clean up the
    // temporary jar files on all hosts, in case some operations
    // failed in the middle of the way ?

    public static final byte CHECK_AND_WRITE = 0;
    public static final byte CLEAN_UP = 1;

    VoltLogger log = new VoltLogger("HOST");

    // Write the new catalog to a temporary jar file
    public CompletableFuture<ClientResponse> run(byte[] catalogBytes, byte mode)
    {
        // This should only be called once on each host
        if (mode == CHECK_AND_WRITE) {
            String err;
            if ((err = VoltDB.instance().checkLoadingClasses(catalogBytes)) != null) {
                return makeQuickResponse(ClientResponseImpl.UNEXPECTED_FAILURE, err);
            }

            try {
                VoltDB.instance().writeCatalogJar(catalogBytes);
            } catch (IOException e) {
                // Catalog disk write failed, include the message
                return makeQuickResponse(ClientResponseImpl.UNEXPECTED_FAILURE, e.getMessage());
            }
        } else if (mode == CLEAN_UP) {
            VoltDB.instance().cleanUpTempCatalogJar();
        } else {
            return makeQuickResponse(ClientResponseImpl.UNEXPECTED_FAILURE, "The mode " + Byte.toString(mode) +
                                     " is not supported in @UpdateCatalogHelper operation.");
        }

        return makeQuickResponse(ClientResponseImpl.SUCCESS, "Catalog update work finished locally with mode " +
                                 Byte.toString(mode));
    }
}
