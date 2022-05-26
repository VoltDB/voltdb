/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package custom_formatter.formatter;

import java.util.Properties;

import org.voltdb.importer.formatter.Formatter;
import org.voltdb.importer.formatter.FormatException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ExampleFormatter implements Formatter {
    /**
     *  Normally the constructor would be used for selecting formatter type
     *  with the formatName argument, and configuring the formatter using the
     *  prop argument.
     *  @param formatName - Name of the formatter to use; in deployment file this is specified by:
     *                      <import><configuration ... format="exampleformatter.jar/[formatName]">
     *  @param prop - Properties for configuring the formatter; in deployment file this is designated with
     *                <format-property ...>...</format-property> below the specification of importer properties.
     */
    Properties m_prop;
    public ExampleFormatter (String formatName, Properties prop) {
        // System.out.println("+++ ExampleFormatter properties: " + prop.propertyNames());
        m_prop = prop;
    }

    /**
     * Function that is called by the importer to handle the transformation.
     * For this example it turns a string into an object array by splitting the string into characters.
     * @param sourceData - Data type of this field is designated by the Formatter<?> data type.
     */
    @Override
    public Object[] transform(ByteBuffer payload) throws FormatException {
        Object[] objs = {"", "", "", "", ""};
        Object[] badobjs1 = {"abc", "def", "123", "", "this one is ok"};
        Object[] badobjs2 = {"", "", "", "", "", ""};

        JSONParser parser = new JSONParser();
        JSONObject jsonObj = null;
        Object seq = null;
        Object instance_id = null;
        Object event_type_id = null;
        Object event_date = null;
        Object trans = null;

        String percentErrors = m_prop.getProperty("errorrate", "1").trim();
        int badinject = 1;
        String sourceData = null;
        try {
            sourceData = new String(payload.array(), payload.arrayOffset(), payload.limit(), StandardCharsets.UTF_8);
            badinject = Integer.parseInt(percentErrors);
        } catch (NumberFormatException e1) {
            e1.printStackTrace();
            badinject = 1;
        }

        try {
            Object obj = parser.parse(sourceData);
            jsonObj = (JSONObject) obj;
            seq = jsonObj.get("seq");
            instance_id =  jsonObj.get("instance_id");
            event_type_id = jsonObj.get("event_type_id");
            event_date = jsonObj.get("event_date");
            trans = jsonObj.get("trans");
            objs[0] = seq;
            objs[1] = instance_id;
            objs[2] = event_type_id;
            objs[3] = event_date;
            objs[4] = trans;
        } catch (Exception e) {
            e.printStackTrace();
        }
        double r = Math.random();
        if (r < (badinject/100.0)) {
            if (r < 1.0/3.0)         // randomize the choice of bad outcomes
                return badobjs1;
            else if (r >= 1.0/3.0 && r < 2.0/3.0)
                return badobjs2;
            throw new FormatException();
        }
        return objs;
    }
}
