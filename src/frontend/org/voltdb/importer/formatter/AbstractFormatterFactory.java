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

package org.voltdb.importer.formatter;

import java.util.Properties;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

/**
 * Factory class that formatter bundles should extend, extending classes
 * should implement a create method to construct the class that implements Formatter.
 */
public abstract class AbstractFormatterFactory implements BundleActivator {

    /**
     * Registers this as an OSGi service. At startup, the server will look
     * for this factory service within the configured importer bundles.
     */
    @Override
    public final void start(BundleContext context) throws Exception {
        context.registerService(AbstractFormatterFactory.class, this, null);
    }

    @Override
    public final void stop(BundleContext context) throws Exception {
        // Nothing to do for now.
    }

    /**
     * Abstract method for constructing the class implementing Formatter
     * @param formatName  the type of the formatter
     * @param props - the properties used for the construction of formatter.
     * @return formatter instance created with the importer id
     */
    public abstract Formatter<?> create(String  formatName, Properties props);
}
