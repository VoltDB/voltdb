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

package org.voltdb.export;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.MockVoltDB;
import org.voltdb.VoltDB;
import org.voltdb.compiler.deploymentfile.FeatureType;
import org.voltdb.compiler.deploymentfile.FeaturesType;
import org.voltdb.export.ExportManagerInterface.ExportMode;
import org.voltdb.export.ExportManagerInterface.SetupException;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;

/**
 * Tests verifying that export feature options specified in deployment are used correctly.
 */
public class TestExportFeatureOption extends JUnit4LocalClusterTest {

    private MockVoltDB m_mockVolt;

    @Before
    public void setup() {
        m_mockVolt = new MockVoltDB();
        VoltDB.replaceVoltDBInstanceForTest(m_mockVolt);
    }

    @After
    public void tearDown() throws Exception {
        m_mockVolt.shutdown(null);
    }

    @Test
    public void testNoOption() throws Exception {
        ExportMode expected = m_mockVolt.getConfig().m_isEnterprise ? ExportMode.ADVANCED : ExportMode.BASIC;
        ExportMode exportMode = ExportManagerInterface.getExportFeatureMode(null);
        assertEquals(expected, exportMode);
        FeaturesType features = new FeaturesType();
        exportMode = ExportManagerInterface.getExportFeatureMode(features);
        assertEquals(expected, exportMode);
    }

    @Test
    public void testBasicOption() throws Exception {
        FeaturesType features = new FeaturesType();
        FeatureType feature = new FeatureType();
        feature.setName(ExportManagerInterface.EXPORT_FEATURE.toUpperCase());
        feature.setOption(ExportManagerInterface.ExportMode.BASIC.name());
        features.getFeature().add(feature);
        ExportMode exportMode = ExportManagerInterface.getExportFeatureMode(features);
        assertEquals(ExportMode.BASIC, exportMode);
    }

    @Test
    public void testAdvancedOption() throws Exception {
        FeaturesType features = new FeaturesType();
        FeatureType feature = new FeatureType();
        feature.setName(ExportManagerInterface.EXPORT_FEATURE);
        feature.setOption(ExportManagerInterface.ExportMode.ADVANCED.name().toLowerCase());
        features.getFeature().add(feature);
        try {
            ExportMode exportMode = ExportManagerInterface.getExportFeatureMode(features);
            assertTrue(m_mockVolt.getConfig().m_isEnterprise);
            assertEquals(ExportMode.ADVANCED, exportMode);
        } catch(SetupException e) {
            assertFalse(m_mockVolt.getConfig().m_isEnterprise);
        }
    }
}
