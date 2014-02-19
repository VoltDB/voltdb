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

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.rights.Grantee;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.lib.HsqlArrayList;

/**
 * Implementation of SQL procedure and functions
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 *
 * @version 1.9.0
 * @since 1.9.0
 */
public class RoutineSchema implements SchemaObject {

    Routine[]        routines = Routine.emptyArray;
    int              routineType;
    private HsqlName name;

    public RoutineSchema(int type, HsqlName name) {
        routineType = type;
        this.name   = name;
    }

    public int getType() {
        return routineType;
    }

    public HsqlName getCatalogName() {
        return name.schema.schema;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public HsqlName getName() {
        return name;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {
        return new OrderedHashSet();
    }

    public OrderedHashSet getComponents() {

        OrderedHashSet set = new OrderedHashSet();

        set.addAll(routines);

        return set;
    }

    public void compile(Session session) {}

    public String getSQL() {
        return null;
    }

    public String[] getSQLArray() {

        HsqlArrayList list = new HsqlArrayList();

        for (int i = 0; i < routines.length; i++) {
            list.add(routines[i].getSQL());
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    public void addSpecificRoutine(Database database, Routine routine) {

        int    signature = routine.getParameterSignature();
        Type[] types     = routine.getParameterTypes();

        for (int i = 0; i < this.routines.length; i++) {
            if (routines[i].parameterTypes.length == types.length) {
                if (routineType == SchemaObject.PROCEDURE) {
                    throw Error.error(ErrorCode.X_42605);
                }

                boolean match = true;

                for (int j = 0; j < types.length; j++) {
                    if (!routines[i].parameterTypes[j].equals(types[j])) {
                        match = false;

                        break;
                    }
                }

                if (match) {
                    throw Error.error(ErrorCode.X_42605);
                }
            }
        }

        if (routine.getSpecificName() == null) {
            HsqlName specificName = database.nameManager.newAutoName("",
                name.name, name.schema, name, name.type);

            routine.setSpecificName(specificName);
        }

        routines = (Routine[]) ArrayUtil.resizeArray(routines,
                routines.length + 1);
        routines[routines.length - 1] = routine;
    }

    public Routine[] getSpecificRoutines() {
        return routines;
    }

    public Routine getSpecificRoutine(Type[] types) {

        int matchIndex = -1;

        outerLoop:
        for (int i = 0; i < this.routines.length; i++) {
            int matchCount = 0;

            if (routines[i].parameterTypes.length != types.length) {
                continue;
            }

            if (types.length == 0) {
                return this.routines[i];
            }

            for (int j = 0; j < types.length; j++) {
                int typeDifference;

                // parameters
                if (types[j] == null) {
                    continue;
                }

                typeDifference =
                    types[j].precedenceDegree(routines[i].parameterTypes[j]);

                if (typeDifference < 0) {
                    continue outerLoop;
                } else if (typeDifference == 0) {
                    if (matchCount == j) {
                        matchCount = j + 1;
                    }
                }
            }

            if (matchCount == types.length) {
                return routines[i];
            }

            if (matchIndex == -1) {
                matchIndex = i;

                continue;
            }

            for (int j = 0; j < types.length; j++) {
                if (types[j] == null) {
                    continue;
                }

                int oldDiff = types[j].precedenceDegree(
                    routines[matchIndex].parameterTypes[j]);
                int newDiff =
                    types[j].precedenceDegree(routines[i].parameterTypes[j]);

                if (oldDiff == newDiff) {
                    continue;
                }

                if (newDiff < oldDiff) {
                    matchIndex = i;
                }

                continue outerLoop;
            }
        }

        if (matchIndex < 0) {
            throw Error.error(ErrorCode.X_42501);
        }

        return routines[matchIndex];
    }

    public Routine getSpecificRoutine(int paramCount) {

        for (int i = 0; i < this.routines.length; i++) {
            if (routines[i].parameterTypes.length == paramCount) {
                return routines[i];
            }
        }

        throw Error.error(ErrorCode.X_42501);
    }
}
