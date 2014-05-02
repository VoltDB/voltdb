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


package org.hsqldb_voltpatches.types;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.SchemaObject;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.Tokens;
import org.hsqldb_voltpatches.SqlInvariants;

/**
 * Implementation of CHARACTER SET objects.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class Charset implements SchemaObject {

    HsqlName        name;
    public HsqlName base;

    public Charset(HsqlName name) {
        this.name = name;
    }

    public int getType() {
        return SchemaObject.CHARSET;
    }

    public HsqlName getName() {
        return name;
    }

    public HsqlName getCatalogName() {
        return name.schema.schema;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {

        OrderedHashSet set = new OrderedHashSet();

        set.add(base);

        return set;
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session) {}

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_CREATE).append(' ').append(
            Tokens.T_CHARACTER).append(' ').append(Tokens.T_SET).append(' ');
        sb.append(name.getSchemaQualifiedStatementName());
        sb.append(' ').append(Tokens.T_AS).append(' ').append(Tokens.T_GET);
        sb.append(' ').append(base.getSchemaQualifiedStatementName());

        return sb.toString();
    }

    public static Charset getDefaultInstance() {
        return SqlInvariants.UTF16;
    }
}
