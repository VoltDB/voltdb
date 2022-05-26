/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.voltdb.catalog.Procedure;

/**
 * Validator that validates per proc validations.
 */
public class InvocationValidator {

    /**
     * Policies used to determine if we can accept an invocation.
     */
    private final Map<String, List<InvocationValidationPolicy>> m_validationpolicies = new HashMap<>();
    //Single param deserialization policy for sysprocs only.
    private final ParameterDeserializationPolicy m_paramdeserializationpolicy = new ParameterDeserializationPolicy(true);
    //Single invocation acceptance policy for replication.
    private final ReplicaInvocationAcceptancePolicy m_replicationpolicy;

    public InvocationValidator(ReplicationRole role) {
        m_replicationpolicy = new ReplicaInvocationAcceptancePolicy(role == ReplicationRole.REPLICA);
        // NOTE: These "policies" are really parameter correctness checks, not permissions
        registerValidationPolicy("@AdHoc", new AdHocAcceptancePolicy(true));
        registerValidationPolicy("@AdHocSpForTest", new AdHocAcceptancePolicy(true));
        registerValidationPolicy("@UpdateApplicationCatalog", new UpdateCatalogAcceptancePolicy(true));
        registerValidationPolicy("@UpdateClasses", new UpdateClassesAcceptancePolicy(true));
    }

    private void registerValidationPolicy(String procName, InvocationValidationPolicy policy) {
        List<InvocationValidationPolicy> policies = m_validationpolicies.get(procName);
        if (policies == null) {
            policies = new ArrayList<>();
            m_validationpolicies.put(procName, policies);
        }
        policies.add(policy);
    }

    public void setReplicationRole(ReplicationRole role) {
        m_replicationpolicy.setMode(role == ReplicationRole.REPLICA);
    }

    public ClientResponseImpl shouldAccept(String name, AuthSystem.AuthUser user,
                                  final StoredProcedureInvocation task,
                                  final Procedure catProc) {
        ClientResponseImpl error = null;

        //Check replication policy
        if ((error = m_replicationpolicy.shouldAccept(user, task, catProc)) != null) {
            return error;
        }

        //Check param deserialization policy only applies to sysprocs
        if ((error = m_paramdeserializationpolicy.shouldAccept(user, task, catProc)) != null) {
            return error;
        }
        //Check validation policies
        List<InvocationValidationPolicy> vpolicies = m_validationpolicies.get(name);
        if (vpolicies != null) {
            for (InvocationValidationPolicy policy : vpolicies) {
                if ((error = policy.shouldAccept(user, task, catProc)) != null) {
                    return error;
                }
            }
        }
        return null;
    }
}
