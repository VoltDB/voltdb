/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.importer;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.CatalogContext;

/**
 * Created by bshaw on 5/26/17.
 */
public class TestImportManager {

    private static final int HOSTID = 0;
    private static final int SITEID = 0;

    private ImportManager m_manager = null;
    private MockChannelDistributer m_mockChannelDistributer = null;
    private ImporterStatsCollector m_statsCollector = null;

    private static CatalogContext buildMockCatalogContext() throws Exception {
        return null; // new CatalogContext(); // TODO this is going to be a lot of work to mock - perhaps look into cheap ways to make real ones?!
    }

    /** Before test setup code.
     * NOTE: ImportManager is a singleton.
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        CatalogContext mockCatalogV1 = buildMockCatalogContext();
        m_mockChannelDistributer = new MockChannelDistributer(Integer.toString(HOSTID));
        m_statsCollector = new ImporterStatsCollector(SITEID);
        ImportManager.initializeWithMocks(HOSTID, mockCatalogV1, m_mockChannelDistributer, m_statsCollector);
        m_manager = ImportManager.instance();
    }

    @Test
    public void testDev() throws Exception {
        //m_manager.
    }
}
