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

package org.voltdb;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.voltdb.catalog.Cluster;
import org.voltdb.settings.NodeSettings;

public class TestNativeLibraryLoader {

    @Test
    public void testLoader() {
        VoltDB.Configuration configuration = new VoltDB.Configuration();
        configuration.m_noLoadLibVOLTDB = true;
        MockVoltDB mockvolt = new MockVoltDB();
        VoltDB.ignoreCrash = true;
        VoltDB.replaceVoltDBInstanceForTest(mockvolt);
        mockvolt.m_noLoadLib = true;
        assertFalse(NativeLibraryLoader.loadVoltDB(false));
        assertFalse(VoltDB.wasCrashCalled);
        boolean threw = false;
        try {
            assertFalse(NativeLibraryLoader.loadVoltDB(true));
        } catch (AssertionError ae) {
            threw = true;
        }
        assertTrue(threw);
        assertTrue(VoltDB.wasCrashCalled);
        VoltDB.wasCrashCalled = false;
        VoltDB.initialize(configuration, true);
        assertFalse(NativeLibraryLoader.loadVoltDB(true));
        assertFalse(VoltDB.wasCrashCalled);

        // Now test SUCCESS case
        configuration = new VoltDB.Configuration();
        VoltDBInterface mockitovolt = mock(VoltDBInterface.class);
        VoltDBInterface realvolt = new RealVoltDB();
        when(mockitovolt.getEELibraryVersionString())
            .thenReturn(realvolt.getEELibraryVersionString());
        CatalogContext catContext = mock(CatalogContext.class);
        Cluster cluster = mock(Cluster.class);
        NodeSettings settings = mock(NodeSettings.class);
        when(catContext.getCluster())
            .thenReturn(cluster);
        when(mockitovolt.getCatalogContext())
            .thenReturn(catContext);
        when(catContext.getNodeSettings())
            .thenReturn(settings);
        when(settings.getLocalSitesCount())
            .thenReturn(1);

        VoltDB.replaceVoltDBInstanceForTest(mockitovolt);
        VoltDB.initialize(configuration, true);
        assertTrue(NativeLibraryLoader.loadVoltDB(true));
    }
}
