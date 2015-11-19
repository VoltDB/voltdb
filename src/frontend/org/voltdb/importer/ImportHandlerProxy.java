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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.client.ProcedureCallback;

/**
 * This is the proxy for real ImportHandler. This is used as real handler
 * has lot of server code dependencies and not really helpful for the import bundle builder.
 * @author akhanzode
 */
public abstract class ImportHandlerProxy implements ImportContext, ChannelChangeCallback {

    public static final int SMALL_STACK_SIZE = 1024 * 256;
    public static final int MEDIUM_STACK_SIZE = 1024 * 512;
    private static final AtomicLong m_createdThreadCount = new AtomicLong(0);

    private Object m_handler = null;
    private Method m_callProcMethod;
    private Method m_asyncCallProcMethod;
    private Method m_hasTableMethod;
    private Method m_info_log;
    private Method m_error_log;
    private Method m_warn_log;
    private Method m_error_log_withT;
    private Method m_error_log_rateLimited;
    private Method m_warn_log_rateLimited;
    private Method m_debug_log;
    private Method m_trace_log;
    private Method m_debug_enabled;
    private Method m_trace_enabled;
    private Method m_info_enabled;
    private Method m_statsFailureCall;
    private Method m_statsQueuedCall;

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

    @Override
    public boolean callProcedure(Invocation invocation) {
        try {
            boolean result = (Boolean )m_callProcMethod.invoke(m_handler, this, invocation.getProcedure(), invocation.getParams());
            reportStat(result, invocation.getProcedure());
            return result;
        } catch (Exception ex) {
            error(ex, "%s: Error trying to import", getName());
            reportFailureStat(invocation.getProcedure());
            return false;
        }
    }

    @Override
    public boolean callProcedure(ProcedureCallback cb, Invocation invocation) {
        try {
            Object params[] = invocation.getParams();
            boolean result = (Boolean )m_asyncCallProcMethod.invoke(m_handler, this, cb, invocation.getProcedure(), params);
            reportStat(result, invocation.getProcedure());
            return result;
        } catch (Exception ex) {
            error(ex, "%s: Error trying to import", getName());
            reportFailureStat(invocation.getProcedure());
            return false;
        }
    }

    private void reportStat(boolean result, String procName) {
        try {
            if (result) {
                m_statsQueuedCall.invoke(m_handler, getName(), procName);
            } else {
                m_statsFailureCall.invoke(m_handler, getName(), procName, false);
            }
        } catch(InvocationTargetException e) {
            warn(e, "Error trying to report importer status to statistics collector");
        } catch(IllegalAccessException e) {
            warn(e, "Error trying to report importer status to statistics collector");
        }
    }

    private void reportFailureStat(String procName) {
        try {
            m_statsFailureCall.invoke(m_handler, getName(), procName, false);
        } catch(InvocationTargetException e) {
            warn(e, "Error trying to report importer failure to statistics collector");
        } catch(IllegalAccessException e) {
            warn(e, "Error trying to report importer failure to statistics collector");
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
        m_trace_log = m_handler.getClass().getMethod("debug", String.class);
        m_error_log_rateLimited = m_handler.getClass().getMethod("rateLimitedError", Throwable.class, String.class, Object[].class);
        m_error_log_withT = m_handler.getClass().getMethod("error", String.class, Throwable.class);
        m_debug_enabled = m_handler.getClass().getMethod("isDebugEnabled", (Class<?>[] )null);
        m_info_enabled = m_handler.getClass().getMethod("isInfoEnabled", (Class<?>[] )null);
        m_trace_enabled = m_handler.getClass().getMethod("isTraceEnabled", (Class<?>[] )null);
        m_warn_log_rateLimited = m_handler.getClass().getMethod("rateLimitedWarn", Throwable.class, String.class, Object[].class);
        m_statsFailureCall = m_handler.getClass().getMethod("reportFailure", String.class, String.class, boolean.class);
        m_statsQueuedCall = m_handler.getClass().getMethod("reportQueued", String.class, String.class);
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
    public void error(Throwable t, String format, Object...args) {
        try {
            if (m_error_log != null) {
                m_error_log_rateLimited.invoke(m_handler, t, format, args);
            }
        } catch (Exception ex) {
        }
    }

    @Override
    public void warn(Throwable t, String format, Object...args) {
        try {
            if (m_warn_log_rateLimited != null) {
                m_warn_log_rateLimited.invoke(m_handler, t, format, args);
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
    public void error(String message, Throwable t) {
        try {
            if (m_error_log != null) {
                m_error_log_withT.invoke(m_handler, message, t);
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
    public void trace(String message) {
        try {
            if (m_debug_log != null) {
                m_trace_log.invoke(m_handler, message);
            }
        } catch (Exception ex) {
        }
    }

    @Override
    public boolean isDebugEnabled() {
        try {
            if (m_debug_enabled != null) {
                return (Boolean )m_debug_enabled.invoke(m_handler);
            }
        } catch (Exception ex) {
        }
        return false;
    }

    @Override
    public boolean isTraceEnabled() {
        try {
            if (m_debug_enabled != null) {
                return (Boolean )m_trace_enabled.invoke(m_handler);
            }
        } catch (Exception ex) {
        }
        return false;
    }

    @Override
    public boolean isInfoEnabled() {
        try {
            if (m_debug_enabled != null) {
                return (Boolean )m_info_enabled.invoke(m_handler);
            }
        } catch (Exception ex) {
        }
        return false;
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
    public void onChange(ImporterChannelAssignment assignment) {
        throw new UnsupportedOperationException("For Distributed Importer this must be implemented.");
    }

    @Override
    public void onClusterStateChange(VersionedOperationMode mode) {
        throw new UnsupportedOperationException("For Distributed Importer this must be implemented.");
    }

    /**
     * Get thread factory for a group with thread names.
     * @param groupName - group name of new ThreadGroup
     * @param name - names of the thread auto incremented so identify each thread in the group
     * @param stackSize - see static definations
     * @return  ThreadFactory to be used for your executor service.
     */
    public static ThreadFactory getThreadFactory(final String groupName, final String name, final int stackSize) {
        final ThreadGroup group = new ThreadGroup(Thread.currentThread().getThreadGroup(), groupName);

        return new ThreadFactory() {
            @Override
            public synchronized Thread newThread(final Runnable r) {
                final String threadName = name + " - " + m_createdThreadCount.getAndIncrement();
                Thread t = new Thread(group, r, threadName, stackSize);
                t.setDaemon(true);
                return t;
            }
        };
    }

}
