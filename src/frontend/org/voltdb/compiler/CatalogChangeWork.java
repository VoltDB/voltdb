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

package org.voltdb.compiler;

import java.util.List;

import org.voltdb.AuthSystem;
import org.voltdb.parser.SQLLexer;
import org.voltdb.parser.SQLParser;
import org.voltdb.utils.Encoder;

public class CatalogChangeWork extends AsyncCompilerWork {
    private static final long serialVersionUID = -5257248292283453286L;

    public static class CatalogChangeParameters {
        public final String[] ddlStmts;
        public final String jarIdentifier;
        // The bytes for the catalog operation, if any.  May be null in all cases
        // For @UpdateApplicationCatalog, this will contain the compiled catalog jarfile bytes
        // For @UpdateClasses, this will contain the class jarfile bytes
        // For @AdHoc DDL work, this will be null
        public final byte[] operationBytes;
        // The string for the catalog operation, if any.  May be null in all cases
        // For @UpdateApplicationCatalog, this will contain the deployment string to apply
        // For @UpdateClasses, this will contain the class deletion patterns
        // For @AdHoc DDL work, this will be null
        public final String operationString;

        public final String errorMessage;

        public CatalogChangeParameters (String[] stmts, String identifier, byte[] operationBytes, String operationString) {
            this.ddlStmts = stmts;
            this.jarIdentifier = identifier;
            this.operationBytes = operationBytes;
            this.operationString = operationString;
            this.errorMessage = null;
        }

        public CatalogChangeParameters (String error) {
            ddlStmts = null;
            jarIdentifier = null;
            operationBytes = null;
            operationString = null;
            errorMessage = error;
        }
    }

    public CatalogChangeParameters ccParams;
    final byte[] replayHashOverride;
    final boolean isPromotion;
    public final long replayTxnId;
    public final long replayUniqueId;

    public CatalogChangeWork(
            long replySiteId,
            long clientHandle, long connectionId, String hostname, boolean adminConnection, Object clientData,
            CatalogChangeParameters ccParams,
            String invocationName, boolean onReplica, boolean useAdhocDDL,
            AsyncCompilerWorkCompletionHandler completionHandler,
            AuthSystem.AuthUser user, byte[] replayHashOverride,
            boolean isPromotion, long replayTxnId, long replayUniqeuId)
    {
        super(replySiteId, false, clientHandle, connectionId, hostname,
              adminConnection, clientData, invocationName,
              onReplica, useAdhocDDL,
              completionHandler, user);
        this.ccParams = ccParams;
        this.replayHashOverride = replayHashOverride;
        this.isPromotion = isPromotion;
        this.replayTxnId = replayTxnId;
        this.replayUniqueId = replayUniqeuId;
    }

    /**
     * To process adhoc DDL, we want to convert the AdHocPlannerWork we received from the
     * ClientInterface into a CatalogChangeWork object for the AsyncCompilerAgentHelper to
     * grind on.
     */
    public CatalogChangeWork(AdHocPlannerWork adhocDDL)
    {
        super(adhocDDL.replySiteId,
              adhocDDL.shouldShutdown,
              adhocDDL.clientHandle,
              adhocDDL.connectionId,
              adhocDDL.hostname,
              adhocDDL.adminConnection,
              adhocDDL.clientData,
              adhocDDL.invocationName,
              adhocDDL.onReplica,
              adhocDDL.useAdhocDDL,
              adhocDDL.completionHandler,
              adhocDDL.user);
        // AsyncCompilerAgentHelper will fill in the current catalog bytes later.
        // Ditto for deployment string
        this.ccParams = new CatalogChangeParameters(adhocDDL.sqlStatements, null, null, null);

        this.replayHashOverride = null;
        this.isPromotion = false;
        this.replayTxnId = -1L;
        this.replayUniqueId = -1L;
    }

    public boolean isForReplay()
    {
        return replayHashOverride != null;
    }

    public static CatalogChangeParameters fromParams(String procName, Object[] paramArray) {
        if (paramArray == null) return null;

        String[] ddlStmts = null;
        String jarIdentifier = null;
        byte[] jarBytes = null;
        String operationString = null;

        // default catalogBytes to null, when passed along, will tell the
        // catalog change planner that we want to use the current catalog.
        if ("@UpdateApplicationCatalog".equals(procName) || "@UpdateClasses".equals(procName)) {
            Object catalogObj = paramArray[0];
            if (catalogObj != null) {
                if (catalogObj instanceof String) {
                    // treat an empty string as no catalog provided
                    String catalogString = (String) catalogObj;
                    if (!catalogString.isEmpty()) {
                        jarBytes = Encoder.hexDecode(catalogString);
                    }
                } else if (catalogObj instanceof byte[]) {
                    // treat an empty array as no catalog provided
                    byte[] catalogArr = (byte[]) catalogObj;
                    if (catalogArr.length != 0) {
                        jarBytes = catalogArr;
                    }
                }
            }
            operationString = (String) paramArray[1];
        }
        else if ("@UpdateApplication".equals(procName)) {
            // input parameters checked
            String errorMsg = checkParameterTypesForUpdateApplication(paramArray);
            if (errorMsg != null) {
                return new CatalogChangeParameters(errorMsg);
            }

            List<String> sqlStatements = null;
            if (paramArray[0] != null) {
                sqlStatements = SQLLexer.splitStatements((String) paramArray[0]);
            }

            if (paramArray[1] == null && paramArray[2] != null) {
                return new CatalogChangeParameters("Input Jar is missing the identifier");
            } else if (paramArray[1] != null && paramArray[2] == null) {
                return new CatalogChangeParameters("Input Jar bytes are missing");
            }

            if (paramArray[1] != null) {
                String[] identifiersArr = (String[]) paramArray[1];
                if (identifiersArr.length != 1) {
                    return new CatalogChangeParameters("Jar indentifier array length expect 1, "
                            + "but received " + identifiersArr.length);
                }
                jarIdentifier = identifiersArr[0];
            }
            if (paramArray[2] != null) {
                byte[][] bytesArr = (byte[][]) paramArray[2];
                if (bytesArr.length != 1) {
                    return new CatalogChangeParameters("Jar bytes array length expect 1, "
                            + "but received " + bytesArr.length);
                }
                jarBytes = bytesArr[0];
            }

            if (paramArray[3] != null) {
                // classes delete pattern strings
                operationString = (String) paramArray[3];
            }

            errorMsg = checkStmtsForUpdateApplication(sqlStatements, jarIdentifier);
            if (errorMsg != null) {
                return new CatalogChangeParameters(errorMsg);
            }

            if (sqlStatements != null) {
                ddlStmts = sqlStatements.toArray(new String[sqlStatements.size()]);
            }
        }

        return new CatalogChangeParameters(ddlStmts, jarIdentifier, jarBytes, operationString);
    }

    private static String checkParameterTypesForUpdateApplication(final Object[] paramArray) {
        if (paramArray.length != 4) {
            return "Expect 4 parameters, but received " + paramArray.length;
        }

        String[] types = {"java.lang.String", "java.lang.String[]", "byte[][]", "java.lang.String"};
        for (int i = 0; i < types.length; i++) {
            String type = types[i];
            if (paramArray[i] != null && ! type.equals(paramArray[i].getClass().getTypeName())) {
                return String.format("Expect %d parameter to be %s, but received %s",
                        i, type, paramArray[i].getClass().getTypeName());
            }
        }
        return null;
    }

    /**
     * The first statement has to be "LOAD CLASSES..." in the statements
     * @param ddlStmts
     * @return
     */
    private static String checkStmtsForUpdateApplication(List<String> ddlStmts, String identifier) {
        if (ddlStmts == null || ddlStmts.size() == 0) {
            if (identifier == null || identifier.trim().length() == 0) {
                return null;
            }
            return "jar identifier not expected: " + identifier;
        }
        String firstStmt = ddlStmts.get(0);
        String arg = SQLParser.parseLoadClassesOnly(firstStmt);
        if (arg == null || arg.trim().length() == 0) {
            if (identifier == null) {
                return null;
            }
            return "Expect 'Load Classes jar identifier', but received: " + firstStmt;
        }
        if (!arg.equals(identifier)) {
            return "jar identifier " + arg + " not found in the identifier input array";
        }

        ddlStmts.remove(0);
        return null;
    }
}
