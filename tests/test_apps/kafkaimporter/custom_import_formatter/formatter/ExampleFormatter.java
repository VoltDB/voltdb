package custom_import_formatter.formatter;

import java.util.Properties;

import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;

public class ExampleFormatter implements Formatter<String> {
    /**
     *  Normally the constructor would be used for selecting formatter type
     *  with the formatName argument, and configuring the formatter using the
     *  prop argument.
     *  @param formatName - Name of the formatter to use; in deployment file this is specified by:
     *                      <import><configuration ... format="exampleformatter.jar/[formatName]">
     *  @param prop - Properties for configuring the formatter; in deployment file this is designated with
     *                <format-property ...>...</format-property> below the specification of importer properties.
     */
    ExampleFormatter (String formatName, Properties prop) {
    }

    /**
     * Function that is called by the importer to handle the transformation.
     * For this example it turns a string into an object array by splitting the string into characters.
     * @param sourceData - Data type of this field is designated by the Formatter<?> data type.
     */
    @Override
    public Object[] transform(String sourceData) throws FormatException {
        Object[] objs = {"", "", ""};
        objs = sourceData.split(",", 3);
        return objs;
    }
}
