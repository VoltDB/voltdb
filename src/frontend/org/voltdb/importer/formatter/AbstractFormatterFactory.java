package org.voltdb.importer.formatter;

import java.util.Properties;

public interface AbstractFormatterFactory {
    public AbstractFormatter create(Properties prop);
}
