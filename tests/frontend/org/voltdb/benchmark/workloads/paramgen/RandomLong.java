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

public class RandomLong extends ParamGenerator
{
    long m_min;
    long m_max;

    public RandomLong(ParamType paramInfo)
    {
        if (paramInfo != null)
        {
            m_min = paramInfo.getBigint().getMin();
            m_max = paramInfo.getBigint().getMax();
        }
        else
        {
            m_min = Long.MIN_VALUE;
            m_max = Long.MAX_VALUE;
        }
    }

    // XXX-IZZY use min/max here
    @Override
    public Object getNextGeneratedValue()
    {
        // XXX-IZZY hacky range hackery
        if ((Math.abs(m_max / 2) + (Math.abs(m_min / 2))) > Long.MAX_VALUE / 2)
        {
            long randomLong = (long)(Math.random() * Long.MAX_VALUE);
            if (Math.random() < 0.5)
                return (long)(randomLong - Long.MAX_VALUE);
            else
                return randomLong;
        }
        else
        {
            return (long)((Math.random() * (m_max - m_min)) + m_min);
        }
    }
}
