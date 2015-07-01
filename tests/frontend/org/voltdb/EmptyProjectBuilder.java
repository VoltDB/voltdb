/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;

import org.voltdb.compiler.VoltProjectBuilder;

/**
 * Test project builder with empty DDL.
 */
public class EmptyProjectBuilder extends VoltProjectBuilder
{
    private static File s_ddlFile = null;
    private static String s_ddlURL = null;

    public EmptyProjectBuilder() throws IOException
    {
        super();
        if (s_ddlFile == null) {
            s_ddlFile = VoltProjectBuilder.writeStringToTempFile("");
            s_ddlFile.deleteOnExit();
            s_ddlURL = URLEncoder.encode(s_ddlFile.getPath(), "UTF-8");
        }
        addSchema(s_ddlURL);
    }
}
