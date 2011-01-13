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

public class HttpContentType {

    public static enum Type {
        HTML, PNG, CSS, JS
    }

    private Type type;

    public HttpContentType(String type) {
        if (type.equals("html")) {
            this.type = Type.HTML;
        } else if (type.equals("png")) {
            this.type = Type.PNG;
        } else if (type.equals("css")) {
            this.type = Type.CSS;
        } else if (type.equals("js")) {
            this.type = Type.JS;
        }
    }

    @Override
    public String toString() {
        switch(type) {
        case HTML:
            return "text/html; charset=UTF-8";
        case PNG:
            return "image/png";
        case CSS:
            return "text/css";
        case JS:
            return "application/javascript";
        default:
            return "";
        }
    }

}
