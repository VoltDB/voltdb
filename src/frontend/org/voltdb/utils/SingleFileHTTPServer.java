/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Given a single file, make it available at a generated URL
 * until shutdown is called.
 */
public class SingleFileHTTPServer extends NanoHTTPD {

    String m_uri;
    String m_mime;
    byte[] m_data;

    /**
     * Protected constructor does little and should only be called by
     * static method serveFile(..)
     */
    SingleFileHTTPServer(int port, String name, String mime, byte[] data) throws IOException {
        super(port);
        m_mime = mime;
        m_data = data;
        m_uri = "http://localhost:" + String.valueOf(port) + "/" + name;
    }

    @Override
    public Response serve(String uri, String method, Properties header, Properties parms) {
        assert(m_data != null);
        assert(m_mime != null);
        ByteArrayInputStream bis = new ByteArrayInputStream(m_data);
        return new Response(HTTP_OK, m_mime, bis);
    }

    public String uri() {
        return m_uri;
    }

    /**
     * Immediately read a files contents into a buffer
     * and create an http server that serves just that one file.
     *
     * @param f The file to serve.
     * @return An instance of the http server.
     */
    public static SingleFileHTTPServer serveFile(File f) {
        assert(f != null);
        if (f.exists() == false) return null;
        if (f.isFile() == false) return null;
        if (f.canRead() == false) return null;

        // read the file into a byte array
        int length = (int) f.length();
        assert(length < (1024*1024*100)); // 100mb reasonable max
        byte[] data = new byte[length];
        try {
            FileInputStream fis = new FileInputStream(f);
            fis.read(data);
        } catch (Exception e1) {
            return null;
        }

        // get the mime type
        String mime = null;
        try {
            mime = NanoHTTPD.mimeFromExtention(f);
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        // get the filename
        String name = f.getName();

        // start up the server on the first free
        // port after 8080
        SingleFileHTTPServer server = null;
        int port = 8080;
        while (server == null) {
            try {
                server = new SingleFileHTTPServer(port++, name, mime, data);
            }
            catch (IOException e) {}
        }

        return server;
    }
}
