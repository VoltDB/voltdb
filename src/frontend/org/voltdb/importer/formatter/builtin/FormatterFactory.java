package org.voltdb.importer.formatter.builtin;

import java.util.Properties;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.voltdb.importer.formatter.AbstractFormatter;
import org.voltdb.importer.formatter.AbstractFormatterFactory;

public class FormatterFactory implements BundleActivator, AbstractFormatterFactory {
    @Override
    public void start(BundleContext context) throws Exception {
        context.registerService(this.getClass().getName(), this, null);
    }

    @Override
    public void stop(BundleContext context) throws Exception {
        //Do any bundle related cleanup.
    }

    @Override
    public AbstractFormatter create(Properties prop) {
        AbstractFormatter formatter = new VoltFormatter();
        formatter.configure(prop);
        return formatter;
    }
}
