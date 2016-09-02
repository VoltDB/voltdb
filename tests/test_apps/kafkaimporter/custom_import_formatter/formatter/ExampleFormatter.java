package custom_import_formatter.formatter;

import java.util.Properties;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.importer.formatter.Formatter;
//import org.json.simple.JSONObject;
//import org.json.simple.JSONArray;
//import org.json.simple.parser.ParseException;
//import org.json.simple.parser.JSONParser;
import org.voltdb.importer.formatter.FormatException;

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

//        obj = new JSONObject();
//        try {
//            obj.put("seq", seq);
//            obj.put("instance_id", instance_id);
//            obj.put("event_type_id", event_type_id);
//            obj.put("event_date", event_date);
//            obj.put("trans",  trans);

        JSONObject jsonObj = null;
        String seq = null;
        String instance_id = null;
        String event_type_id = null;
        String event_date = null;
        String trans = null;
        try {
            jsonObj = new JSONObject(sourceData);
             seq = (String) jsonObj.get("seq");
             instance_id = (String) jsonObj.get("instance_id");
             event_type_id = (String) jsonObj.get("event_type_id");
             event_date = (String) jsonObj.get("event_date");
             trans = (String) jsonObj.get("trans");
        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Object[] objs = {seq, instance_id, event_type_id, event_date, trans};
        return objs;
    }
}
