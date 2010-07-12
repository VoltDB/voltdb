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


package org.hsqldb_voltpatches.util;

import java.sql.Types;

// fredt@users 20020215 - patch 516309 by Nicolas Bazin - transfer Informix
// sqlbob@users 20020325 - patch 1.7.0 - reengineering

/**
 * Conversions from Informix databases
 *
 * @author Nichola Bazin
 * @version 1.7.0
 */
class InformixTransferHelper extends TransferHelper {

    public InformixTransferHelper() {
        super();
    }

    public InformixTransferHelper(TransferDb database, Traceable t,
                                  String q) {
        super(database, t, q);
    }

    void setSchema(String _Schema) {
        sSchema = "\"" + _Schema + "\"";
    }

    int convertFromType(int type) {

        //Correct a bug in Informix JDBC driver that maps:
        // DATETIME YEAR TO FRACTION to TIME and
        // DATETIME HOUR TO SECOND to TIMESTAMP
        if (type == Types.TIMESTAMP) {
            type = Types.TIME;

            tracer.trace("Converted INFORMIX TIMESTAMP to TIME");
        } else if (type == Types.TIME) {
            type = Types.TIMESTAMP;

            tracer.trace("Converted INFORMIX TIME to TIMESTAMP");
        }

        return (type);
    }

    int convertToType(int type) {

        //Correct a bug in Informix JDBC driver that maps:
        // DATETIME YEAR TO FRACTION to TIME and
        // DATETIME HOUR TO SECOND to TIMESTAMP
        if (type == Types.TIMESTAMP) {
            type = Types.TIME;

            tracer.trace("Converted TIMESTAMP to INFORMIX TIME");
        } else if (type == Types.TIME) {
            type = Types.TIMESTAMP;

            tracer.trace("Converted TIME to INFORMIX TIMESTAMP");
        }

        return (type);
    }
}
