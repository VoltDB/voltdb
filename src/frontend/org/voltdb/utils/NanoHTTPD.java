/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/*
Copyright (C) 2001,2005-2010 by Jarno Elonen <elonen@iki.fi>

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer. Redistributions in
binary form must reproduce the above copyright notice, this list of
conditions and the following disclaimer in the documentation and/or other
materials provided with the distribution. The name of the author may not
be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package org.voltdb.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A simple, tiny, nicely embeddable HTTP 1.0 server in Java
 *
 * <p> NanoHTTPD version 1.14,
 * Copyright &copy; 2001,2005-2010 Jarno Elonen (elonen@iki.fi, http://iki.fi/elonen/)
 *
 * <p><b>Features + limitations: </b><ul>
 *
 *    <li> Only one Java file </li>
 *    <li> Java 1.1 compatible </li>
 *    <li> Released as open source, Modified BSD licence </li>
 *    <li> No fixed config files, logging, authorization etc. (Implement yourself if you need them.) </li>
 *    <li> Supports parameter parsing of GET and POST methods </li>
 *    <li> Supports both dynamic content and file serving </li>
 *    <li> Never caches anything </li>
 *    <li> Doesn't limit bandwidth, request time or simultaneous connections </li>
 *    <li> Default code serves files and shows all HTTP parameters and headers</li>
 *    <li> File server supports directory listing, index.html and index.htm </li>
 *    <li> File server does the 301 redirection trick for directories without '/'</li>
 *    <li> File server supports simple skipping for files (continue download) </li>
 *    <li> File server uses current directory as a web root </li>
 *    <li> File server serves also very long files without memory overhead </li>
 *    <li> Contains a built-in list of most common mime types </li>
 *    <li> All header names are converted lowercase so they don't vary between browsers/clients </li>
 *
 * </ul>
 */
public abstract class NanoHTTPD {

    final static int THREADPOOL_SIZE = 3;

    int myTcpPort;
    final ServerSocket myServerSocket;
    Thread serverThread;
    HTTPWorkerThread threadPool[] = new HTTPWorkerThread[THREADPOOL_SIZE];
    LinkedBlockingQueue<Work> workQueue = new LinkedBlockingQueue<Work>();

    class Work {
        Socket socket = null;
        Response response = null;
        boolean shouldDie = false;
    }

    // ==================================================
    // API parts
    // ==================================================

    public class Request {
        final public String uri;
        final public String method;
        final public Properties header;
        final public Properties parms;

        final Socket socket;

        Request(String uri, String method, Properties header, Properties parms, Socket socket) {
            this.uri = uri;
            this.method = method;
            this.header = header;
            this.parms = parms;
            this.socket = socket;
        }
    }

    /**
     * HTTP response.
     * Return one of these from serve().
     */
    public class Response
    {
        /** HTTP status code after processing, e.g. "200 OK", HTTP_OK */
        public final String status;

        /** MIME type of content, e.g. "text/html" */
        public final String mimeType;

        /** Data of the response, may be null. */
        public final InputStream data;

        /**
         * Headers for the HTTP response. Use addHeader()
         * to add lines.
         */
        public final Properties header = new Properties();

        /**
         * Basic constructor.
         */
        public Response(String status, String mimeType, InputStream data) {
            this.status = status;
            this.mimeType = mimeType;
            this.data = data;
        }

        /**
         * Convenience method that makes an InputStream out of
         * given text.
         */
        public Response(String status, String mimeType, String txt) {
            this.status = status;
            this.mimeType = mimeType;
            try {
                this.data = new ByteArrayInputStream( txt.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("JVM is missing UTF-8 encoding support. Therefore JVM is not supported.");
            }
        }

        /**
         * Adds given line to the header.
         */
        public void addHeader(String name, String value) {
            header.put(name, value);
        }
    }

    /**
     * Override this to customize the server.<p>
     *
     * @parm uri    Percent-decoded URI without parameters, for example "/index.cgi"
     * @parm method "GET", "POST" etc.
     * @parm parms  Parsed, percent decoded parameters from URI and, in case of POST, data.
     * @parm header Header entries, percent decoded
     * @return HTTP response, see class Response for details or null if request will asynchronously complete.
     */
    public abstract Response processRequest(Request request) throws Exception;

    public void completeRequest(Request request, Response response) {
        assert(request != null);
        assert(response != null);

        Work w = new Work();
        w.socket = request.socket;
        w.response = response;
        try {
            workQueue.put(w);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Some HTTP response status codes
     */
    public static final String
        HTTP_OK = "200 OK",
        HTTP_REDIRECT = "301 Moved Permanently",
        HTTP_FORBIDDEN = "403 Forbidden",
        HTTP_NOTFOUND = "404 Not Found",
        HTTP_BADREQUEST = "400 Bad Request",
        HTTP_INTERNALERROR = "500 Internal Server Error",
        HTTP_NOTIMPLEMENTED = "501 Not Implemented";

    /**
     * Common mime types for dynamic content
     */
    public static final String
        MIME_PLAINTEXT = "text/plain; charset=utf-8",
        MIME_HTML = "text/html; charset=utf-8",
        MIME_DEFAULT_BINARY = "application/octet-stream; charset=utf-8";

    // ==================================================
    // Socket & server code
    // ==================================================

    /**
     * Starts a HTTP server to given port.<p>
     * Throws an IOException if the socket is already in use
     */
    public NanoHTTPD( int port ) throws IOException
    {
        for (int i = 0; i < threadPool.length; i++) {
            threadPool[i] = new HTTPWorkerThread();
            threadPool[i].start();
        }

        myTcpPort = port;
        myServerSocket = new ServerSocket( myTcpPort );
        serverThread = new Thread( new Runnable()
            {
                public void run()
                {
                    try
                    {
                        while( true ) {
                            Work w = new Work();
                            w.socket = myServerSocket.accept();
                            workQueue.add(w);
                        }
                    }
                    catch ( IOException ioe )
                    {}
                }
            });
        serverThread.setDaemon( true );
        serverThread.start();
    }

    /**
     * Stops the server.
     */
    public void stop(boolean blocking)
    {
        try
        {
            myServerSocket.close();
            for (int i = 0; i < threadPool.length; i++) {
                Work w = new Work();
                w.shouldDie = true;
                workQueue.put(w);
            }
            if (blocking) {
                serverThread.join();
                for (HTTPWorkerThread worker : threadPool) {
                    worker.join();
                }
            }
        }
        catch ( IOException ioe ) {}
        catch ( InterruptedException e ) {}
    }

    class HTTPWorkerThread extends Thread {

        @Override
        public void run() {
            while(true) {
                Work work = null;
                try {
                    work = workQueue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (work == null)
                    continue;

                if (work.shouldDie) {
                    assert(work.socket == null);
                    assert(work.response == null);
                    return;
                }

                if (work.response == null) {
                    assert(work.socket != null);
                    handleNewSocket(work.socket);
                }
                else {
                    sendResponse(work.socket,
                                 work.response.status,
                                 work.response.mimeType,
                                 work.response.header,
                                 work.response.data);
                }
            }
        }

        void handleNewSocket(Socket socket) {
            try
            {
                InputStream is = socket.getInputStream();
                if ( is == null) return;
                BufferedReader in = new BufferedReader( new InputStreamReader( is ));

                // Read the request line
                String inLine = in.readLine();
                if (inLine == null) return;
                StringTokenizer st = new StringTokenizer( inLine );
                if ( !st.hasMoreTokens())
                    sendError(socket, HTTP_BADREQUEST, "BAD REQUEST: Syntax error. Usage: GET /example/file.html" );

                String method = st.nextToken();

                if ( !st.hasMoreTokens())
                    sendError(socket, HTTP_BADREQUEST, "BAD REQUEST: Missing URI. Usage: GET /example/file.html" );

                String uri = st.nextToken();

                // Decode parameters from the URI
                Properties parms = new Properties();
                int qmi = uri.indexOf( '?' );
                if ( qmi >= 0 )
                {
                    decodeParms(socket, uri.substring( qmi+1 ), parms );
                    uri = URLDecoder.decode(uri.substring( 0, qmi ), "UTF-8");
                }
                else uri = URLDecoder.decode(uri, "UTF-8");


                // If there's another token, it's protocol version,
                // followed by HTTP headers. Ignore version but parse headers.
                // NOTE: this now forces header names uppercase since they are
                // case insensitive and vary by client.
                Properties header = new Properties();
                if ( st.hasMoreTokens())
                {
                    String line = in.readLine();
                    while ( line.trim().length() > 0 )
                    {
                        int p = line.indexOf( ':' );
                        header.put( line.substring(0,p).trim().toLowerCase(), line.substring(p+1).trim());
                        line = in.readLine();
                    }
                }

                // If the method is POST, there may be parameters
                // in data section, too, read it:
                if ( method.equalsIgnoreCase( "POST" ))
                {
                    long size = 0x7FFFFFFFFFFFFFFFl;
                    String contentLength = header.getProperty("content-length");
                    if (contentLength != null)
                    {
                        try { size = Integer.parseInt(contentLength); }
                        catch (NumberFormatException ex) {}
                    }
                    String postLine = "";
                    char buf[] = new char[512];
                    int read = in.read(buf);
                    while ( read >= 0 && size > 0 && !postLine.endsWith("\r\n") )
                    {
                        size -= read;
                        postLine += String.valueOf(buf, 0, read);
                        if ( size > 0 )
                            read = in.read(buf);
                    }
                    postLine = postLine.trim();
                    decodeParms(socket, postLine, parms );
                }

                // Ok, now do the processRequest()
                try {
                    Response r = processRequest(new Request(uri, method, header, parms, socket));
                    if (r != null)
                        sendResponse(socket, r.status, r.mimeType, r.header, r.data);
                }
                catch (Exception e) {
                    sendError(socket, HTTP_INTERNALERROR, "SERVER INTERNAL ERROR: processRequest() threw an exception." );
                }

                in.close();
            }
            catch ( IOException ioe )
            {
                try
                {
                    sendError(socket, HTTP_INTERNALERROR, "SERVER INTERNAL ERROR: IOException: " + ioe.getMessage());
                }
                catch ( Throwable t ) {}
            }
            catch ( InterruptedException ie )
            {
                // Thrown by sendError, ignore and exit the thread.
            }
        }

        /**
         * Decodes parameters in percent-encoded URI-format
         * ( e.g. "name=Jack%20Daniels&pass=Single%20Malt" ) and
         * adds them to given Properties. NOTE: this doesn't support multiple
         * identical keys due to the simplicity of Properties -- if you need multiples,
         * you might want to replace the Properties with a Hastable of Vectors or such.
         * @throws UnsupportedEncodingException
         */
        private void decodeParms(Socket socket, String parms, Properties p ) throws InterruptedException {
            if ( parms == null )
                return;

            try {
                StringTokenizer st = new StringTokenizer( parms, "&" );
                while ( st.hasMoreTokens())
                {
                    String e = st.nextToken();
                    int sep = e.indexOf( '=' );
                    if ( sep >= 0 )
                        p.put( URLDecoder.decode( e.substring( 0, sep ), "UTF-8").trim(),
                               URLDecoder.decode( e.substring( sep+1 ), "UTF-8"));
                }
            }
            catch (Exception e) {
                sendError(socket, HTTP_BADREQUEST, "BAD REQUEST: Bad URL encoding." );
            }
        }

        /**
         * Returns an error message as a HTTP response and
         * throws InterruptedException to stop furhter request processing.
         */
        private void sendError(Socket socket, String status, String msg) throws InterruptedException {
            sendResponse(socket, status, MIME_PLAINTEXT, null, new ByteArrayInputStream( msg.getBytes()));
            throw new InterruptedException();
        }

        /**
         * Sends given response to the socket.
         */
        private void sendResponse(Socket socket, String status, String mime, Properties header, InputStream data)
        {
            try
            {
                if ( status == null )
                    throw new Error( "sendResponse(): Status can't be null." );

                OutputStream out = socket.getOutputStream();
                PrintWriter pw = new PrintWriter( out );
                pw.print("HTTP/1.0 " + status + " \r\n");

                if ( mime != null )
                    pw.print("Content-Type: " + mime + "\r\n");

                if ( header == null || header.getProperty( "Date" ) == null )
                    pw.print( "Date: " + gmtFrmt.format( new Date()) + "\r\n");

                if ( header != null )
                {
                    Enumeration<Object> e = header.keys();
                    while ( e.hasMoreElements())
                    {
                        String key = (String) e.nextElement();
                        String value = header.getProperty( key );
                        pw.print( key + ": " + value + "\r\n");
                    }
                }

                pw.print("\r\n");
                pw.flush();

                if ( data != null )
                {
                    byte[] buff = new byte[2048];
                    while (true)
                    {
                        int read = data.read( buff, 0, 2048 );
                        if (read <= 0)
                            break;
                        out.write( buff, 0, read );
                    }
                }
                out.flush();
                out.close();
                if ( data != null )
                    data.close();
            }
            catch( IOException ioe )
            {
                // Couldn't write? No can do.
                try { socket.close(); } catch( Throwable t ) {}
            }
        }

    }

    public static String mimeFromExtention(File f) throws IOException {
        String mime = null;
        int dot = f.getCanonicalPath().lastIndexOf( '.' );
        if ( dot >= 0 )
            mime = theMimeTypes.get( f.getCanonicalPath().substring( dot + 1 ).toLowerCase());
        if ( mime == null )
            mime = MIME_DEFAULT_BINARY;
        return mime;
    }

    /**
     * Hashtable mapping (String)FILENAME_EXTENSION -> (String)MIME_TYPE
     */
    private static Hashtable<String, String> theMimeTypes = new Hashtable<String, String>();
    static
    {
        StringTokenizer st = new StringTokenizer(
            "htm        text/html "+
            "html       text/html "+
            "txt        text/plain "+
            "asc        text/plain "+
            "gif        image/gif "+
            "jpg        image/jpeg "+
            "jpeg       image/jpeg "+
            "png        image/png "+
            "mp3        audio/mpeg "+
            "m3u        audio/mpeg-url " +
            "pdf        application/pdf "+
            "doc        application/msword "+
            "ogg        application/x-ogg "+
            "zip        application/octet-stream "+
            "exe        application/octet-stream "+
            "class      application/octet-stream " );
        while ( st.hasMoreTokens())
            theMimeTypes.put( st.nextToken(), st.nextToken());
    }

    /**
     * GMT date formatter
     */
    private static java.text.SimpleDateFormat gmtFrmt;
    static
    {
        gmtFrmt = new java.text.SimpleDateFormat( "E, d MMM yyyy HH:mm:ss 'GMT'", Locale.US);
        gmtFrmt.setTimeZone(TimeZone.getTimeZone("GMT"));
    }
}
