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

import java.lang.reflect.Method;
import java.util.Properties;


/**
 * This is the proxy for real ImportHandler. This is used as real handler
 * has lot of server code dependencies and not really helpful for the import bundle builder.
 * @author akhanzode
 */
public class ImportHandlerProxy implements ImportContext {

    private Object m_handler = null;
    private Method m_invoker;
    private Method m_info_log;
    private Method m_error_log;

    /**
     * These must be implemented in tghe bundle even if you are not using any properties.
     * @param p
     */
    @Override
    public void configure(Properties p) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * This must be implemented in bundle if ready for data returns the importer will stop.
     */
    @Override
    public void readyForData() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * This is called to cleanup the bundle and must be implemented. Its also invoked when the
     * catalog or deployment is updated.
     */
    @Override
    public void stop() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * This calls real handler using reflection.
     * @param ic
     * @param proc
     * @param fieldList
     * @return
     */
    @Override
    public boolean callProcedure(ImportContext ic, String proc, Object... fieldList) {
        try {
            return (Boolean )m_invoker.invoke(m_handler, ic, proc, fieldList);
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    @Override
    public void setHandler(Object handler) {
        m_handler = handler;
        try {
            m_invoker = m_handler.getClass().getMethod("callProcedure", ImportContext.class, String.class, Object[].class);
            m_info_log = m_handler.getClass().getMethod("info", String.class);
            m_error_log = m_handler.getClass().getMethod("error", String.class);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void info(String message) {
        try {
            if (m_info_log != null) {
                m_info_log.invoke(m_handler, message);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    @Override
    public void error(String message) {
        try {
            if (m_error_log != null) {
                m_error_log.invoke(m_handler, message);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
