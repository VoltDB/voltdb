/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.exportclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

public class TestJDBCExportClient extends ExportClientTestBase {
    @Test
    public void testConfigValidation() throws Exception
    {
        final JDBCExportClient client = new JDBCExportClient();
        final Properties config = new Properties();
        try {
            client.configure(config);
            fail("Empty config");
        } catch (Exception e) {}

        config.setProperty("jdbcurl", "fakeurl");
        config.setProperty("jdbcuser", "fakeuser");
        config.setProperty("jdbcdriver", "org.voltdb.export.JDBCDriverForTest");

        try {
            client.configure(config);
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testDefaultConfig() throws Exception
    {
        final JDBCExportClient client = new JDBCExportClient();
        final Properties config = new Properties();
        config.setProperty("jdbcurl", "fakeurl");
        config.setProperty("jdbcuser", "fakeuser");
        config.setProperty("jdbcdriver", "org.voltdb.export.JDBCDriverForTest");
        try {
            client.configure(config);
            assertEquals("fakeurl", client.m_poolProperties.getUrl());
            assertEquals("fakeuser", client.m_poolProperties.getUsername());
            assertNull("", client.m_poolProperties.getPassword());
            assertEquals("", client.schema_prefix);
            assertEquals("org.apache.tomcat.jdbc.pool.interceptor.StatementCache(max=50)", client.m_poolProperties.getJdbcInterceptors());
            assertTrue(client.m_poolProperties.isTestOnBorrow());
            assertEquals("SELECT 1", client.m_poolProperties.getValidationQuery());
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testPoolSize() throws Exception
    {
        final JDBCExportClient client = new JDBCExportClient();
        final Properties config = new Properties();
        config.setProperty("jdbcurl", "fakeurl");
        config.setProperty("jdbcuser", "fakeuser");
        config.setProperty("jdbcdriver", "org.voltdb.export.JDBCDriverForTest");
        config.setProperty("minpoolsize", "4");
        config.setProperty("maxpoolsize", "10");
        config.setProperty("maxidletime", "60");
        try {
            client.configure(config);
            assertEquals(4, client.m_poolProperties.getMinIdle());
            assertEquals(10, client.m_poolProperties.getMaxIdle());
            assertEquals(60, client.m_poolProperties.getMinEvictableIdleTimeMillis());
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testInvalidPoolSize()
    {
        final JDBCExportClient client = new JDBCExportClient();
        Properties config = new Properties();
        config.setProperty("jdbcurl", "fakeurl");
        config.setProperty("jdbcuser", "fakeuser");
        config.setProperty("jdbcdriver", "org.voltdb.export.JDBCDriverForTest");
        config.setProperty("minpoolsize", "4l");
        try {
            client.configure(config);
            fail("Invalid min pool size");
        } catch (Exception e) {}
        finally {
            client.shutdown();
        }

        config = new Properties();
        config.setProperty("jdbcurl", "fakeurl");
        config.setProperty("jdbcuser", "fakeuser");
        config.setProperty("jdbcdriver", "org.voltdb.export.JDBCDriverForTest");
        config.setProperty("maxpoolsize", "4l");
        try {
            client.configure(config);
            fail("Invalid max pool size");
        } catch (Exception e) {}
        finally {
            client.shutdown();
        }

        config = new Properties();
        config.setProperty("jdbcurl", "fakeurl");
        config.setProperty("jdbcuser", "fakeuser");
        config.setProperty("jdbcdriver", "org.voltdb.export.JDBCDriverForTest");
        config.setProperty("maxidletime", "4l");
        try {
            client.configure(config);
            fail("Invalid max pool size");
        } catch (Exception e) {}
        finally {
            client.shutdown();
        }
    }

    @Test
    public void testPropIdentifiers() throws Exception
    {
        final JDBCExportClient client1 = new JDBCExportClient();
        final Properties config1 = new Properties();
        config1.setProperty("jdbcurl", "fakeurl");
        config1.setProperty("jdbcuser", "fakeuser");
        config1.setProperty("jdbcdriver", "org.voltdb.export.JDBCDriverForTest");
        config1.setProperty("createtable", "true");
        client1.configure(config1);

        assertEquals(JDBCExportClient.m_cpds.get().size(), 1);
        assertEquals(JDBCExportClient.m_cpds.get().get(client1.m_urlId).getRefCount(), 1);
        assertTrue(client1.m_createTable);

        final JDBCExportClient client2 = new JDBCExportClient();
        final Properties config2 = new Properties();
        config2.setProperty("jdbcurl", "fakeurl");
        config2.setProperty("jdbcuser", "fakeuser");
        config2.setProperty("jdbcdriver", "org.voltdb.export.JDBCDriverForTest");
        config2.setProperty("createtable", "false");
        client2.configure(config2);

        assertEquals(JDBCExportClient.m_cpds.get().size(), 1);
        assertEquals(JDBCExportClient.m_cpds.get().get(client2.m_urlId).getRefCount(), 2);
        assertFalse(client2.m_createTable);

        final JDBCExportClient client3 = new JDBCExportClient();
        final Properties config3 = new Properties();
        config3.setProperty("jdbcurl", "fakeurl");
        config3.setProperty("jdbcuser", "fakeuser2");
        config3.setProperty("jdbcdriver", "org.voltdb.export.JDBCDriverForTest");
        client3.configure(config3);

        assertEquals(JDBCExportClient.m_cpds.get().size(), 2);
        assertEquals(JDBCExportClient.m_cpds.get().get(client3.m_urlId).getRefCount(), 1);

        client1.shutdown();
        assertEquals(JDBCExportClient.m_cpds.get().size(), 2);
        assertEquals(JDBCExportClient.m_cpds.get().get(client2.m_urlId).getRefCount(), 1);
        client2.shutdown();
        assertEquals(JDBCExportClient.m_cpds.get().size(), 1);
        client3.shutdown();
        assertEquals(JDBCExportClient.m_cpds.get().size(), 0);
    }

    @Test
    public void testLowerCaseNames() throws Exception
    {
        final JDBCExportClient client = new JDBCExportClient();
        final Properties config = new Properties();

        config.setProperty("jdbcurl", "fakeurl");
        config.setProperty("jdbcuser", "fakeuser");
        config.setProperty("jdbcdriver", "org.voltdb.export.JDBCDriverForTest");
        config.setProperty("lowercase", "true");

        try {
            client.configure(config);
        } finally {
            client.shutdown();
        }
    }
}
