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

import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;


/**
 * TODO:
 */
public abstract class AbstractImporterFactory implements BundleActivator {

    private ImporterServerAdapter m_importServerAdapter;

    /**
     * Registers this as an OSGi service.
     */
    @Override
    public final void start(BundleContext context) throws Exception {
        context.registerService(this.getClass().getName(), this, null);
    }

    @Override
    public final void stop(BundleContext context) throws Exception {
        // Nothing to do for now.
    }

    public final void setImportServerAdapter(ImporterServerAdapter importServerAdapter)
    {
        m_importServerAdapter = importServerAdapter;
    }

    public final AbstractImporter createImporter(ImporterConfig config)
    {
        AbstractImporter importer = create(config);
        importer.setImportServerAdapter(m_importServerAdapter);
        return importer;
    }

    protected abstract AbstractImporter create(ImporterConfig config);

    public abstract String getTypeName();

    public abstract Map<URI, ImporterConfig> createImporterConfigurations(Properties props);

    public abstract boolean isImporterRunEveryWhere();
}
