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

import java.math.BigDecimal;

import org.voltdb.benchmark.workloads.xml.ParamType;

public class RandomDecimal extends ParamGenerator
{
    int m_scale;
    int m_precision;
    BigDecimal m_min;
    BigDecimal m_max;

    public RandomDecimal(ParamType paramInfo)
    {
        if (paramInfo != null)
        {
            m_scale = paramInfo.getDecimal().getScale();
            m_precision = paramInfo.getDecimal().getPrecision();
            m_min = paramInfo.getDecimal().getMin();
            m_max = paramInfo.getDecimal().getMax();
        }
        else
        {
            m_scale = 12;
            m_precision = 38;
            m_min = new BigDecimal("-99999999999999999999999999.999999999999");
            m_max = new BigDecimal("99999999999999999999999999.999999999999");
        }
    }

    // XXX-IZZY use scale/precision/min/max here
    @Override
    public Object getNextGeneratedValue()
    {
        String temp = "";
        if (Math.random() < 0.5)
            temp += "-";
        for (int i = 0; i < 26; i++)
            temp += (int)(Math.random() * 10);
        temp += ".";
        for (int i = 0; i < 12; i++)
            temp += (int)(Math.random() * 10);
        return new BigDecimal(temp);
    }
}
