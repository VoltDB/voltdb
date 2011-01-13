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

package org.voltdb.benchmark.workloads.paramgen;

import org.voltdb.benchmark.workloads.xml.ParamType;

public class RandomString extends ParamGenerator
{
    private int m_length;

    public RandomString(ParamType paramInfo)
    {
        if (paramInfo != null)
        {
            m_length = paramInfo.getVarchar().getLength();
        }
        else
        {
            m_length = 32767; // random large default string size
        }
    }

    @Override
    public String getNextGeneratedValue()
    {
        String randomString = "";

        double rand = Math.random();
        while ((double)randomString.length() / m_length < rand)
        {
            randomString += getChar();
            rand = Math.random();
        }

        return randomString;
    }

    char getChar()
    {
        char ASCIIChar;
        ASCIIChar = (char)((int)(Math.random() * 95) + 32);
        return ASCIIChar;
    }
}
