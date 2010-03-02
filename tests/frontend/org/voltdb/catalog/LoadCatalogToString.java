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

package org.voltdb.catalog;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Scanner;

/** just reads catalog.txt. */
public class LoadCatalogToString {
    public static final String THE_CATALOG;
    static {
        try {
            URL url = LoadCatalogToString.class.getResource("catalog.txt");
            String catPath = URLDecoder.decode(url.getPath(), "UTF-8");

            Scanner scanner = new Scanner(new File(catPath));
            String value = "";
            while(scanner.hasNextLine())
                value += scanner.nextLine() + "\n";

            THE_CATALOG = value;
            //System.out.println("Catalog to be tested:\n" + THE_CATALOG);
        } catch (Exception e) {
            System.err.println("FAILED TO LOAD CATALOG!");
            throw new RuntimeException(e);
        }
    }
    public static void main(String[] args) {
        System.err.println("CATALOG : " + LoadCatalogToString.THE_CATALOG);
    }
}
