/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.importer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.voltdb.client.ProcedureCallback;

/**
 * This is the proxy for real ImportHandler. This is used as real handler
 * has lot of server code dependencies and not really helpful for the import bundle builder.
 * @author akhanzode
 */
public abstract class ImportHandlerProxy implements ImportContext, ChannelChangeCallback {

    private Object m_handler = null;
    private Method m_callProcMethod;
    private Method m_asyncCallProcMethod;
    private Method m_hasTableMethod;
    private Method m_info_log;
    private Method m_error_log;
    private Method m_warn_log;
    private Method m_error_log_withT;
    private Method m_debug_log;

    @Override
    public boolean canContinue() {
        return true;
    }

    public boolean hasTable(String name) {
        try {
            return (Boolean) m_hasTableMethod.invoke(m_handler, name);
        } catch(InvocationTargetException e) { // this shouldn't happen
            throw new RuntimeException(e);
        } catch(IllegalAccessException e) { // this shouldn't happen
            throw new RuntimeException(e);
        }
    }

    /**
     * This calls real handler using reflection.
     * @param ic
     * @param proc
     * @param fieldList
     * @return
     */
    @Override
    public boolean callProcedure(String proc, Object... fieldList) {
        try {
            return (Boolean )m_callProcMethod.invoke(m_handler, this, proc, fieldList);
        } catch (Exception ex) {
            return false;
        }
    }

    @Override
    public boolean callProcedure(Invocation invocation) {
        try {
            Object params[] = invocation.getParams();
            return (Boolean )m_callProcMethod.invoke(m_handler, this, invocation.getProcedure(), params);
        } catch (Exception ex) {
            return false;
        }
    }

    @Override
    public boolean callProcedure(ProcedureCallback cb, Invocation invocation) {
        try {
            Object params[] = invocation.getParams();
            return (Boolean )m_asyncCallProcMethod.invoke(m_handler, this, cb, invocation.getProcedure(), params);
        } catch (Exception ex) {
            return false;
        }
    }

    @Override
    public void setHandler(Object handler) throws Exception {
        m_handler = handler;
        m_callProcMethod = m_handler.getClass().getMethod("callProcedure", ImportContext.class, String.class, Object[].class);
        m_asyncCallProcMethod = m_handler.getClass().getMethod("callProcedure", ImportContext.class, ProcedureCallback.class, String.class, Object[].class);
        m_hasTableMethod = m_handler.getClass().getMethod("hasTable", String.class);
        m_info_log = m_handler.getClass().getMethod("info", String.class);
        m_error_log = m_handler.getClass().getMethod("error", String.class);
        m_warn_log = m_handler.getClass().getMethod("warn", String.class);
        m_debug_log = m_handler.getClass().getMethod("debug", String.class);
        m_error_log_withT = m_handler.getClass().getMethod("error", String.class, Throwable.class);
    }

    @Override
    public void info(String message) {
        try {
            if (m_info_log != null) {
                m_info_log.invoke(m_handler, message);
            }
        } catch (Exception ex) {
        }
    }

    @Override
    public void error(String message, Throwable t) {
        try {
            if (m_error_log != null) {
                m_error_log_withT.invoke(m_handler, message, t);
            }
        } catch (Exception ex) {
        }
    }

    @Override
    public void error(String message) {
        try {
            if (m_error_log != null) {
                m_error_log.invoke(m_handler, message);
            }
        } catch (Exception ex) {
        }
    }

    @Override
    public void warn(String message) {
        try {
            if (m_error_log != null) {
                m_warn_log.invoke(m_handler, message);
            }
        } catch (Exception ex) {
        }
    }

    @Override
    public void debug(String message) {
        try {
            if (m_debug_log != null) {
                m_debug_log.invoke(m_handler, message);
            }
        } catch (Exception ex) {
        }
    }

    @Override
    public long getBackpressureTimeout() {
        return TimeUnit.MINUTES.toNanos(2);
    }

    @Override
    public boolean isRunEveryWhere() {
        return true;
    }

    @Override
    public Set<URI> getAllResponsibleResources() {
        throw new UnsupportedOperationException("For Distributed Importer this must be implemented.");
    }

    @Override
    public void onChange(Set<URI> added, Set<URI> removed, Set<URI> assigned, int version) {
        throw new UnsupportedOperationException("For Distributed Importer this must be implemented.");
    }
}
