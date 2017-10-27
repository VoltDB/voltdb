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

package org.voltdb.utils;

import com.google_voltpatches.common.cache.Cache;
import com.google_voltpatches.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;
import org.eclipse.jetty.server.session.AbstractSessionCache;
import org.eclipse.jetty.server.session.Session;
import org.eclipse.jetty.server.session.SessionData;
import org.eclipse.jetty.server.session.SessionHandler;

/**
 *
 * @author akhanzode
 */
public class HTTPSessionDataCache extends AbstractSessionCache {

    //Cache of 100 session objects values as Weak as we only compare keys.
    Cache<String, Session> m_cache = CacheBuilder.newBuilder()
            .expireAfterAccess(60,TimeUnit.MINUTES)
            .weakValues()
            .maximumSize(100).build();
    public HTTPSessionDataCache (SessionHandler manager)
    {
        super (manager);
    }

    @Override
    public Session newSession(SessionData data) {
        Session s =  new Session(getSessionHandler(), data);
        return m_cache.asMap().put(s.getId(), s);
    }

    @Override
    public Session newSession(HttpServletRequest request, SessionData data) {
        Session s =  new Session(getSessionHandler(),request, data);
        return m_cache.asMap().put(s.getId(), s);
    }

    @Override
    public Session doGet(String id) {
        if (id == null)
            return null;

        return m_cache.asMap().get(id);
    }

    @Override
    public Session doPutIfAbsent(String id, Session sn) {
        if (id == null) return null;
        return m_cache.asMap().putIfAbsent(id, sn);
    }

    @Override
    public boolean doReplace(String id, Session sn1, Session sn2) {
        if (id == null) return false;

        return m_cache.asMap().replace(id, sn1, sn2);
    }

    @Override
    public Session doDelete(String id) {
        return m_cache.asMap().remove(id);
    }

    @Override
    public void shutdown() {
        synchronized (m_cache) {
            m_cache.cleanUp();
        }
    }

}
