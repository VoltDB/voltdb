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

package org.voltdb;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.voltcore.messaging.Mailbox;
import org.voltcore.network.Connection;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Procedure;

import com.google_voltpatches.common.collect.ImmutableMap;

public class LoadedNTProcedureSet {

    // user procedures.
    ImmutableMap<String, ProcedureRunnerNTGenerator> m_procs = ImmutableMap.<String, ProcedureRunnerNTGenerator>builder().build();
    ConcurrentHashMap<Long, ProcedureRunnerNT> m_outstanding = new ConcurrentHashMap<>();
    final InternalConnectionHandler m_ich;
    final ExecutorService m_executorService;
    final Mailbox m_mailbox;

    long nextProcedureRunnerId = 0;

    class ProcedureRunnerNTGenerator {

        protected final String m_procedureName;
        protected final Class<? extends VoltProcedureNT> m_procClz;
        protected final Method m_procMethod;
        protected final Class<?>[] m_paramTypes;

        ProcedureRunnerNTGenerator(Class<? extends VoltProcedureNT> clz) {
            m_procClz = clz;
            m_procedureName = m_procClz.getSimpleName();

            // reflect
            Method procMethod = null;
            Class<?>[] paramTypes = null;

            Method[] methods = m_procClz.getDeclaredMethods();

            for (final Method m : methods) {
                String name = m.getName();
                if (name.equals("run")) {
                    if (Modifier.isPublic(m.getModifiers()) == false) {
                        continue;
                    }
                    procMethod = m;
                    paramTypes = m.getParameterTypes();
                }
            }

            m_procMethod = procMethod;
            m_paramTypes = paramTypes;
        }

        ProcedureRunnerNT generateProcedureRunnerNT(AuthUser user, Connection ccxn, long clientHandle) {
            long id = nextProcedureRunnerId++;

            VoltProcedureNT procedure = null;
            try {
                procedure = m_procClz.newInstance();
            } catch (InstantiationException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            ProcedureRunnerNT runner = new ProcedureRunnerNT(id,
                                                             user,
                                                             ccxn,
                                                             clientHandle,
                                                             procedure,
                                                             m_procedureName,
                                                             m_procMethod,
                                                             m_paramTypes,
                                                             m_executorService,
                                                             LoadedNTProcedureSet.this,
                                                             m_mailbox);
            m_outstanding.put(id, runner);
            return runner;
        }

    }

    LoadedNTProcedureSet(InternalConnectionHandler ich,
                         ExecutorService executorService,
                         Mailbox mailbox)
    {
        assert(ich != null);
        m_ich = ich;
        m_executorService = executorService;
        m_mailbox = mailbox;
    }

    @SuppressWarnings("unchecked")
    void update(CatalogContext catalogContext) {
        CatalogMap<Procedure> procedures = catalogContext.database.getProcedures();

        Map<String, ProcedureRunnerNTGenerator> runnerGeneratorMap = new TreeMap<>();

        for (Procedure procedure : procedures) {
            if (procedure.getTransactional()) {
                continue;
            }

            String className = procedure.getClassname();
            Class<? extends VoltProcedureNT> clz = null;
            try {
                clz = (Class<? extends VoltProcedureNT>) catalogContext.classForProcedure(className);
            }
            catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            ProcedureRunnerNTGenerator prntg = new ProcedureRunnerNTGenerator(clz);
            runnerGeneratorMap.put(procedure.getTypeName(), prntg);
        }

        m_procs = ImmutableMap.<String, ProcedureRunnerNTGenerator>builder().putAll(runnerGeneratorMap).build();
    }

    ClientResponseImpl callProcedureNT(AuthUser user,
                                       final Connection ccxn,
                                       final long clientHandle,
                                       String procName,
                                       ParameterSet paramListIn) {
        ProcedureRunnerNTGenerator prntg = m_procs.get(procName);
        ProcedureRunnerNT runner = prntg.generateProcedureRunnerNT(user, ccxn, clientHandle);
        return runner.submitCall(paramListIn);
    }

    void handleCallbacksForFailedHosts(final Set<Integer> failedHosts) {
        for (ProcedureRunnerNT runner : m_outstanding.values()) {
            runner.processAnyCallbacksFromFailedHosts(failedHosts);
        }
    }
}
