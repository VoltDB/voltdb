/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.twitter.server;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.voltdb.twitter.util.Timestamp;

public class HttpResponse {

    private HttpStatus status;
    private HttpContentType contentType;
    private ByteArrayOutputStream payload;

    // static resources
    public HttpResponse(File file, String contentType) {
        this.status = new HttpStatus(200);
        this.contentType = new HttpContentType(contentType);
        this.payload = new ByteArrayOutputStream();

        // read the file
        InputStream reader = null;
        try {
            reader = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        byte[] bytes = new byte[(int) file.length()];
        try {
            reader.read(bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // write the payload
        try {
            payload.write(bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // dynamic resources
    public HttpResponse(List<String> data, String contentType) {
        this.status = new HttpStatus(200);
        this.contentType = new HttpContentType(contentType);
        this.payload = new ByteArrayOutputStream();

        for (String line : data) {
            try {
                payload.write(line.getBytes());
                payload.write('\n');
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // flushes payload with appropriate headers and closes the output stream
    public void send(OutputStream outputStream) {
        try {
            headers(outputStream);
            payload.writeTo(outputStream);
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void headers(OutputStream outputStream) throws IOException {
        ByteArrayOutputStream headers = new ByteArrayOutputStream();
        headers.write(("HTTP/1.1 " + status + "\n").getBytes());
        headers.write(("Date: " + new Timestamp(Timestamp.RFC1123) + "\n").getBytes());
        headers.write(("Server: VoltDB" + "\n").getBytes());
        headers.write(("Content-Length: " + payload.size() + "\n").getBytes());
        headers.write(("Content-Type: " + contentType + "\n").getBytes());
        headers.write('\n');
        headers.writeTo(outputStream);
    }

}
