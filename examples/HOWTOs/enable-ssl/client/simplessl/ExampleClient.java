/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package simplessl;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.StringTokenizer;

import javax.crypto.Cipher;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.*;


public class ExampleClient
{
    static final String[] firstNames = { "Adam", "Bob", "Clara", "Denise", "Edgar", "Flores", "Gregory" };
    static final String[] lastNames = { "Alloway", "Bregman", "Clauss", "Denisova", "Esmerelda", "Frank", "Gehling"};

    static String generateName(){
        String firstName = firstNames[ (int) (Math.random() * firstNames.length) ];
        String lastName = lastNames[ (int) (Math.random() * lastNames.length) ];
        return firstName + " " + lastName;
    }

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class Parameters extends CLIConfig {
        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "SSL properties file.")
        String sslFile = "";

        @Override
        public void validate() {
            if (sslFile.equals("")){
                exitWithMessageAndUsage("must provide SSL configuration file");
            }
        }
    }

    /** Used to warn the user if strong cryptography is not installed.
     * @return Maximum allowed key length
     */
    public static int getMaxKeyLength() {
        try {
            return Cipher.getMaxAllowedKeyLength("AES/CBC/PKCS5Padding");
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException("AES/CBC/PKCS5Padding is not available, despite it being mandatory for Java implementations!", e);
        }
    }

    public static void main(String[] args)
    {
        final int maxCryptoKeyLength = getMaxKeyLength();
        if (maxCryptoKeyLength < Integer.MAX_VALUE){
            System.err.println("WARNING: Cryptographic key length is limited to " + maxCryptoKeyLength + " bits. You probably don't have unlimited strength cryptography installed.");
        }

        Parameters config = new Parameters();
        config.parse(Client.class.getName(), args);

        System.out.println("Connecting to VoltDB");
        try {
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setTrustStoreConfigFromPropertyFile(config.sslFile);
            clientConfig.enableSSL();

            final Client client = ClientFactory.createClient(clientConfig);
            StringTokenizer serverParser = new StringTokenizer(config.servers, ",");
            while (serverParser.hasMoreTokens()){
                client.createConnection(serverParser.nextToken(), Client.VOLTDB_SERVER_PORT);
            }

            String person = generateName();
            ClientResponse response = client.callProcedure("@AdHoc", "INSERT INTO people VALUES '" + person + "';");
            if (response.getStatus() != ClientResponse.SUCCESS){
                throw new RuntimeException("Could not insert " + person + " into the database", null);
            }
            System.out.println("Added " + person + " to the database");

            response = client.callProcedure("@AdHoc", "SELECT * FROM people LIMIT 10;");
            if (response.getStatus() != ClientResponse.SUCCESS){
                throw new RuntimeException("Could not examine the database");
            }
            VoltTable[] results = response.getResults();
            final int nameIndex = results[0].getColumnIndex("name");

            System.out.println("\nNames in the database: (limit 10)");
            while (results[0].advanceRow()){
                System.out.println(results[0].get(nameIndex, VoltType.STRING));
            }
            
            client.drain();
            client.close();

            System.out.println("Success!");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        catch (ProcCallException e) {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e){
            throw new RuntimeException(e);
        }
    }
}
