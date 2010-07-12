/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2009, The HSQL Development Group
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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Vector;

// sqlbob@users 20020407 - patch 1.7.0 - reengineering

/**
 * Common code in Swing and AWT versions of Tranfer
 * New class based on Hypersonic code
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.7.2
 * @since Hypersonic SQL
 */
class TransferCommon {

    static void savePrefs(String f, DataAccessPoint sourceDb,
                          DataAccessPoint targetDb, Traceable tracer,
                          Vector tTable) {

        TransferTable t;

        try {
            FileOutputStream   fos = new FileOutputStream(f);
            ObjectOutputStream oos = new ObjectOutputStream(fos);

            for (int i = 0; i < tTable.size(); i++) {
                t          = (TransferTable) tTable.elementAt(i);
                t.sourceDb = null;
                t.destDb   = null;
                t.tracer   = null;
            }

            oos.writeObject(tTable);

            for (int i = 0; i < tTable.size(); i++) {
                t          = (TransferTable) tTable.elementAt(i);
                t.tracer   = tracer;
                t.sourceDb = (TransferDb) sourceDb;
                t.destDb   = targetDb;
            }
        } catch (IOException e) {
            System.out.println("pb in SavePrefs : " + e.toString());
            e.printStackTrace();
        }
    }

    static Vector loadPrefs(String f, DataAccessPoint sourceDb,
                            DataAccessPoint targetDb, Traceable tracer) {

        TransferTable     t;
        Vector            tTable = null;
        ObjectInputStream ois    = null;

        try {
            FileInputStream fis = new FileInputStream(f);

            ois    = new ObjectInputStream(fis);
            tTable = (Vector) ois.readObject();

            for (int i = 0; i < tTable.size(); i++) {
                t          = (TransferTable) tTable.elementAt(i);
                t.tracer   = tracer;
                t.sourceDb = (TransferDb) sourceDb;
                t.destDb   = targetDb;
            }
        } catch (ClassNotFoundException e) {
            System.out.println("class not found pb in LoadPrefs : "
                               + e.toString());

            tTable = new Vector();
        } catch (IOException e) {
            System.out.println("IO pb in LoadPrefs : actionPerformed"
                               + e.toString());

            tTable = new Vector();
        } finally {
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException ioe) {}
            }
        }

        return (tTable);
    }

    private TransferCommon() {}
}
