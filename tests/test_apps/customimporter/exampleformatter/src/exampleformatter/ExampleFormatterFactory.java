package exampleformatter;

import java.util.Properties;

import org.voltdb.importer.formatter.AbstractFormatterFactory;
import org.voltdb.importer.formatter.Formatter;

public class ExampleFormatterFactory extends AbstractFormatterFactory {
    /**
     * Creates and returns the formatter object.
     */

    @Override
    public Formatter<String> create(String m_formatName, Properties m_formatProps) {
        ExampleFormatter formatter = new ExampleFormatter(m_formatName, m_formatProps);
        return formatter;
    }
}
