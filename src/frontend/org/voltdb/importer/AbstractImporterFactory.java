/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import org.voltdb.importer.formatter.FormatterBuilder;


/**
 * Factory class that importer bundles should extend to make the importer available
 * for use within VoltDB server.
 * If the importer is made available using an OSGi bundle, this will register itself as a service using
 * BundleActivator.start implementation.
 *
 * @author manjujames
 */
public abstract class AbstractImporterFactory implements BundleActivator
{

    private ImporterServerAdapter m_importServerAdapter;

    /**
     * Registers this as an OSGi service. At startup, the server will look
     * for this factory service within the configured importer bundles.
     */
    @Override
    public final void start(BundleContext context) throws Exception
    {
        context.registerService(AbstractImporterFactory.class, this, null);
    }

    @Override
    public final void stop(BundleContext context) throws Exception
    {
        // Nothing to do for now.
    }

    /**
     * Passes in the adapter class that the importers may use to execute procedures.
     *
     * @param importServerAdapter which adapter is used for invoking procedures from importer.
     */
    public final void setImportServerAdapter(ImporterServerAdapter importServerAdapter)
    {
        m_importServerAdapter = importServerAdapter;
    }

    /**
     * Method that is used by the importer framework classes to create
     * an importer instance and wire it correctly for use within the server.
     *
     * @param config configuration information required to create an importer instance
     * @return importer instance created for the given configuration
     */
    public final AbstractImporter createImporter(ImporterConfig config)
    {
        AbstractImporter importer = create(config);
        importer.setImportServerAdapter(m_importServerAdapter);
        return importer;
    }

    /**
     * This must be implemented by concrete classes to create an instance of the specific importer type.
     *
     * @param config configuration information required to create an importer instance
     * @return importer instance created for the given configuration
     */
    protected abstract AbstractImporter create(ImporterConfig config);

    /**
     * A unique name identifying the type of this importer. This must be unique within the server.
     *
     * @return unique name identifying the type of this importer
     */
    public abstract String getTypeName();

    /**
     * From a given set of properties, creates ImporterConfig for every resource ID configured.
     *
     * @param props importer configuration properties; typically defined in deployment file.
     * @param formatterBuilder  the formatter builder
     * @return Map of resourceIDs to ImporterConfig as configured using the input properties
     */
    public abstract Map<URI, ImporterConfig> createImporterConfigurations(Properties props, FormatterBuilder formatterBuilder);

    /**
     * Returns true if an importer instance must be run on every site for every resource.
     * Returns false if the resources must be distributed between available sites.
     * @return Returns true if importer needs to be run in distributed fashion on all nodes.
     * This also means the importer is asked for URIs to distribute.
     */
    public abstract boolean isImporterRunEveryWhere();
}
