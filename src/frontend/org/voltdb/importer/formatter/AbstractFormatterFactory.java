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
    public final void start(BundleContext context) throws Exception
    {
        context.registerService(this.getClass().getName(), this, null);
    }

    @Override
    public final void stop(BundleContext context) throws Exception
    {
        // Nothing to do for now.
    }

    /**
     * Abstract method for constructing the class implementing Formatter
     * @param name - name of formatter service
     * @param prop - formatter properties
     * @return formatter instance created with the given name and properties
     */
    public abstract Formatter<?> create(String name, Properties prop);

    public final Formatter<?> create(Properties prop) {
        return create(null, prop);
    }
}
