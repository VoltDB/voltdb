package custom_formatter.formatter;

import java.util.Properties;

import org.voltdb.importer.formatter.Formatter;
import org.voltdb.importer.formatter.FormatException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

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
        Object[] objs = {"", "", "", "", ""};

        JSONParser parser = new JSONParser();
        JSONObject jsonObj = null;
        Object seq = null;
        Object instance_id = null;
        Object event_type_id = null;
        Object event_date = null;
        Object trans = null;

        try {
            Object obj = parser.parse(sourceData);
            jsonObj = (JSONObject) obj;
            seq = jsonObj.get("seq");
            instance_id =  jsonObj.get("instance_id");
            event_type_id = jsonObj.get("event_type_id");
            event_date = jsonObj.get("event_date");
            trans = jsonObj.get("trans");
            // System.out.println(sourceData);
            // System.out.println("\t" + seq);
            // System.out.println("\t" + instance_id);
            // System.out.println("\t" + event_type_id);
            // System.out.println("\t" + event_date);
            // System.out.println("\t" + trans);
            objs[0] = seq;
            objs[1] = instance_id;
            objs[2] = event_type_id;
            objs[3] = event_date;
            objs[4] = trans;
        } catch (Exception e) {
            e.printStackTrace();
        }
		return objs;
    }
}
