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

/**
 * @author Nicolas BAZIN, INGENICO
 * @version 1.7.0
 */

// brian.porter@siteforce.de 20020703 - added reference to OracleTransferHelper
class HelperFactory {

    HelperFactory() {}

    // TransferHelper factory
    static TransferHelper getHelper(String productLowerName) {

        TransferHelper f = null;

        if (productLowerName.indexOf("hsql database") != -1) {
            f = new HsqldbTransferHelper();
        } else if (productLowerName.indexOf("postgresql") != -1) {
            f = new PostgresTransferHelper();
        } else if (productLowerName.indexOf("mckoi") != -1) {
            f = new McKoiTransferHelper();
        } else if (productLowerName.indexOf("informix") != -1) {
            f = new InformixTransferHelper();
        } else if (productLowerName.indexOf("oracle") != -1) {
            System.out.println("using the Oracle helper");

            f = new OracleTransferHelper();
        } else if (productLowerName.equals("access")
                   || (productLowerName.indexOf("microsoft") != -1)) {
            f = new SqlServerTransferHelper();
        } else {
            f = new TransferHelper();
        }

        return (f);
    }
}
