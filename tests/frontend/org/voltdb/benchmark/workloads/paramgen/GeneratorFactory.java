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
import org.voltdb.types.TimestampType;

public class GeneratorFactory
{
    public static ParamGenerator getParamGenerator(Class<?> desiredType,
                                                   ParamType paramInfo)
    {
        ParamGenerator new_generator = null;

        if (paramInfo == null)
        {
            System.err.println("No parameter info provided, defaulting to random");
            new_generator = getRandomGenerator(desiredType, paramInfo);
        }
        else if (paramInfo.getSequencegenerator() != null)
        {
            new_generator = getSequenceGenerator(desiredType, paramInfo);
        }
        else if (paramInfo.getUsergenerator() != null)
        {
            System.err.println("CAN'T BUILD USER GENERATOR YET!");
            throw new RuntimeException("CAN'T BUILD USER GENERATOR YET!");
        }
        else
        {
            new_generator = getRandomGenerator(desiredType, paramInfo);
        }

        return new_generator;
    }

    static ParamGenerator getRandomGenerator(Class<?> desiredType,
                                             ParamType paramInfo)
    {
        if (desiredType == String.class)
        {
            return new RandomString(paramInfo);
        }
        else if (desiredType == double.class)
        {
            return new RandomDouble(paramInfo);
        }
        else if (desiredType == BigDecimal.class)
        {
            return new RandomDecimal(paramInfo);
        }
        else if (desiredType == long.class)
        {
            return new RandomLong(paramInfo);
        }
        else if (desiredType == int.class)
        {
            return new RandomInt(paramInfo);
        }
        else if (desiredType == short.class)
        {
            return new RandomShort(paramInfo);
        }
        else if (desiredType == byte.class)
        {
            return new RandomByte(paramInfo);
        }
        else if (desiredType == TimestampType.class)
        {
            return new RandomTimestamp(paramInfo);
        }
        else
        {
            System.err.println("UNSUPPORTED RANDOM GENERATOR TYPE: " + desiredType.toString());
            throw new RuntimeException("UNSUPPORTED RANDOM GENERATOR TYPE: " + desiredType.toString());
        }
    }

    static ParamGenerator getSequenceGenerator(Class<?> desiredType,
                                               ParamType paramInfo)
    {
        if (desiredType == int.class)
        {
            return new SequentialInt(paramInfo);
        }
        else
        {
            System.err.println("UNSUPPORTED SEQUENCE GENERATOR TYPE: " + desiredType.toString());
            throw new RuntimeException("UNSUPPORTED SEQUENCE GENERATOR TYPE: " + desiredType.toString());
        }
    }
}
