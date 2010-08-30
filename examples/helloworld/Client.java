/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
import org.voltdb.*;
import org.voltdb.client.*;

public class Client {

    public static void main(String[] args) throws Exception {

        /*
         * Instantiate a client and connect to the database.
         */
        org.voltdb.client.Client myApp;
        myApp = ClientFactory.createClient();
        myApp.createConnection("localhost");

        /*
         * Load the database.
         */
        myApp.callProcedure("Insert", "Hello", "World", "English");
        myApp.callProcedure("Insert", "Bonjour", "Monde", "French");
        myApp.callProcedure("Insert", "Hola", "Mundo", "Spanish");
        myApp.callProcedure("Insert", "Hej", "Verden", "Danish");
        myApp.callProcedure("Insert", "Ciao", "Mondo", "Italian");

        /*
         * Retrieve the message.
         */
        final ClientResponse response = myApp.callProcedure("Select",
                                                            "Spanish");
        if (response.getStatus() != ClientResponse.SUCCESS){
            System.err.println(response.getStatusString());
            System.exit(-1);
        }

        final VoltTable results[] = response.getResults();
        if (results.length == 0 || results[0].getRowCount() != 1) {
            System.out.printf("I can't say Hello in that language.\n");
            System.exit(-1);
        }

        VoltTable resultTable = results[0];
        VoltTableRow row = resultTable.fetchRow(0);
        System.out.printf("%s, %s!\n", row.getString("hello"),
                                       row.getString("world"));
    }
}
