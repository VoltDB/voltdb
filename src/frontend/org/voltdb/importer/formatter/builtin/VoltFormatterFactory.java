package org.voltdb.importer.formatter.builtin;

import java.util.Properties;

import org.voltdb.importer.formatter.AbstractFormatterFactory;

public class VoltFormatterFactory extends AbstractFormatterFactory {
    @Override
    public VoltFormatter create(String name, Properties prop) {
        VoltFormatter formatter = new VoltFormatter(name, prop);
        return formatter;
    }
}
