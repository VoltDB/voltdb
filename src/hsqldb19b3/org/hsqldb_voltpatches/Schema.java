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


package org.hsqldb_voltpatches;

import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.WrapperIterator;

public final class Schema implements SchemaObject {

    HsqlName        name;
    SchemaObjectSet triggerLookup;
    SchemaObjectSet constraintLookup;
    SchemaObjectSet indexLookup;
    SchemaObjectSet tableLookup;
    SchemaObjectSet sequenceLookup;
    SchemaObjectSet typeLookup;
    SchemaObjectSet charsetLookup;
    SchemaObjectSet collationLookup;
    SchemaObjectSet procedureLookup;
    SchemaObjectSet functionLookup;
    SchemaObjectSet assertionLookup;
    HashMappedList  tableList;
    HashMappedList  sequenceList;
    Grantee         owner;

    Schema(HsqlName name, Grantee owner) {

        this.name        = name;
        triggerLookup    = new SchemaObjectSet(SchemaObject.TRIGGER);
        indexLookup      = new SchemaObjectSet(SchemaObject.INDEX);
        constraintLookup = new SchemaObjectSet(SchemaObject.CONSTRAINT);
        tableLookup      = new SchemaObjectSet(SchemaObject.TABLE);
        sequenceLookup   = new SchemaObjectSet(SchemaObject.SEQUENCE);
        typeLookup       = new SchemaObjectSet(SchemaObject.TYPE);
        charsetLookup    = new SchemaObjectSet(SchemaObject.CHARSET);
        collationLookup  = new SchemaObjectSet(SchemaObject.COLLATION);
        procedureLookup  = new SchemaObjectSet(SchemaObject.PROCEDURE);
        functionLookup   = new SchemaObjectSet(SchemaObject.FUNCTION);
        assertionLookup  = new SchemaObjectSet(SchemaObject.ASSERTION);
        tableList        = (HashMappedList) tableLookup.map;
        sequenceList     = (HashMappedList) sequenceLookup.map;
        this.owner       = owner;
        name.owner       = owner;
    }

    public int getType() {
        return SchemaObject.SCHEMA;
    }

    public HsqlName getName() {
        return name;
    }

    public HsqlName getSchemaName() {
        return null;
    }

    public HsqlName getCatalogName() {
        return null;
    }

    public Grantee getOwner() {
        return owner;
    }

    public OrderedHashSet getReferences() {
        return new OrderedHashSet();
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session) {}

    public String getSQL() {

        StringBuffer sb = new StringBuffer(128);

        sb.append(Tokens.T_CREATE).append(' ');
        sb.append(Tokens.T_SCHEMA).append(' ');
        sb.append(name.statementName).append(' ');
        sb.append(Tokens.T_AUTHORIZATION).append(' ');
        sb.append(owner.getStatementName());

        return sb.toString();
    }

    public String[] getSQLArray(OrderedHashSet resolved,
                                OrderedHashSet unresolved) {

        HsqlArrayList list = new HsqlArrayList();
        StringBuffer  sb   = new StringBuffer(128);

        sb.append(Tokens.T_CREATE).append(' ');
        sb.append(Tokens.T_SCHEMA).append(' ');
        sb.append(name.statementName).append(' ');
        sb.append(Tokens.T_AUTHORIZATION).append(' ');
        sb.append(owner.getStatementName());
        list.add(sb.toString());
        sb.setLength(0);
        sb.append(Tokens.T_SET).append(' ');
        sb.append(Tokens.T_SCHEMA).append(' ');
        sb.append(name.statementName);
        list.add(sb.toString());

        //
        String[] subList;

        subList = charsetLookup.getSQL(resolved, unresolved);

        list.addAll(subList);

        subList = collationLookup.getSQL(resolved, unresolved);

        list.addAll(subList);

        subList = typeLookup.getSQL(resolved, unresolved);

        list.addAll(subList);

        subList = sequenceLookup.getSQL(resolved, unresolved);

        list.addAll(subList);

        subList = tableLookup.getSQL(resolved, unresolved);

        list.addAll(subList);

        subList = functionLookup.getSQL(resolved, unresolved);

        list.addAll(subList);

        subList = procedureLookup.getSQL(resolved, unresolved);

        list.addAll(subList);

        subList = assertionLookup.getSQL(resolved, unresolved);

        list.addAll(subList);

//
        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    public String[] getSequenceRestartSQL() {

        HsqlArrayList list = new HsqlArrayList();
        Iterator      it   = sequenceLookup.map.values().iterator();

        while (it.hasNext()) {
            NumberSequence sequence = (NumberSequence) it.next();
            String         ddl      = sequence.getRestartSQL();

            list.add(ddl);
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    public String[] getTriggerSQL() {

        HsqlArrayList list = new HsqlArrayList();
        Iterator      it   = tableLookup.map.values().iterator();

        while (it.hasNext()) {
            Table    table = (Table) it.next();
            String[] ddl   = table.getTriggerSQL();

            list.addAll(ddl);
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    boolean isEmpty() {
        return sequenceList.isEmpty() && tableList.isEmpty();
    }

    Iterator schemaObjectIterator(int type) {

        switch (type) {

            case SchemaObject.SEQUENCE :
                return sequenceLookup.map.values().iterator();

            case SchemaObject.TABLE :
            case SchemaObject.VIEW :
                return tableLookup.map.values().iterator();

            case SchemaObject.CHARSET :
                return charsetLookup.map.values().iterator();

            case SchemaObject.COLLATION :
                return collationLookup.map.values().iterator();

            case SchemaObject.PROCEDURE :
                return procedureLookup.map.values().iterator();

            case SchemaObject.FUNCTION :
                return functionLookup.map.values().iterator();

            case SchemaObject.ROUTINE :
                Iterator functions = functionLookup.map.values().iterator();

                return new WrapperIterator(
                    functions, procedureLookup.map.values().iterator());

            case SchemaObject.DOMAIN :
            case SchemaObject.TYPE :
                return typeLookup.map.values().iterator();

            case SchemaObject.ASSERTION :
                return assertionLookup.map.values().iterator();

            case SchemaObject.TRIGGER :
                return triggerLookup.map.values().iterator();

            case SchemaObject.INDEX :
            case SchemaObject.CONSTRAINT :
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "SchemaManager");
        }
    }

    void clearStructures() {

        tableList.clear();
        sequenceList.clear();

        triggerLookup    = null;
        indexLookup      = null;
        constraintLookup = null;
        procedureLookup  = null;
        functionLookup   = null;
        sequenceLookup   = null;
        tableLookup      = null;
        typeLookup       = null;
    }
}
