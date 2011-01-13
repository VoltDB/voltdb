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

import org.voltdb.types.TimestampType;

public class RandomValues
{
    private static double coinToss = .5;

    public static Object getRandomValue(Class<?> desiredType)
    {
        if (desiredType == String.class)
        {
            // XXX-IZZY NEED TO REINTRODUCE LENGTH HERE
            int length = 100;
            return getString(length);
        }
        else if (desiredType == double.class)
        {
            return getDouble();
        }
        else if (desiredType == BigDecimal.class)
        {
            return getBigDecimal();
        }
        else if (desiredType == long.class)
        {
            return getLong();
        }
        else if (desiredType == int.class)
        {
            return getInt();
        }
        else if (desiredType == short.class)
        {
            return getShort();
        }
        else if (desiredType == byte.class)
        {
            return getByte();
        }
        else if (desiredType == TimestampType.class)
        {
            return getTimestamp();
        }
        else
        {
            System.err.println("UNKNOWN PARAMETER TYPE: " + desiredType.toString());
            throw new RuntimeException("UNKNOWN PARAMETER TYPE: " + desiredType.toString());
        }

    }

    //improve randomization of length
    public static String getString(int maxLength)
    {
        String randomString = "";

        double rand = Math.random();
        while ((double)randomString.length() / maxLength < rand)
        {
            randomString += getChar();
            rand = Math.random();
        }

        return randomString;
    }

    public static char getChar()
    {
        char ASCIIChar;
        ASCIIChar = (char)((int)(Math.random() * 95) + 32);
        return ASCIIChar;
    }

    public static byte getByte()
    {
        byte randomByte = (byte)(Math.random() * Byte.MAX_VALUE);
        if (Math.random() < coinToss)
            return (byte)(randomByte - Byte.MAX_VALUE);
        else
            return randomByte;
    }

    public static short getShort()
    {
        short randomShort = (short)(Math.random() * Short.MAX_VALUE);
        if (Math.random() < coinToss)
            return (short)(randomShort - Short.MAX_VALUE);
        else
            return randomShort;
    }

    public static long getLong()
    {
        long randomLong = (long)(Math.random() * Long.MAX_VALUE);
        if (Math.random() < coinToss)
            return (randomLong - Long.MAX_VALUE);
        else
            return randomLong;
    }

    public static int getInt()
    {
        int randomInt = (int)(Math.random() * Integer.MAX_VALUE);
        if (Math.random() < coinToss)
            return (randomInt - Integer.MAX_VALUE);
        else
            return randomInt;
    }

    public static double getDouble()
    {
        double randomDouble = (Math.random() * Double.MAX_VALUE);
        if (Math.random() < coinToss)
            return (randomDouble - Double.MAX_VALUE);
        else
            return randomDouble;
    }

    //CHECK FOR PRECISION CORRECTNESS
    public static BigDecimal getBigDecimal()
    {
        String temp = "";
        if (Math.random() < coinToss)
            temp += "-";
        for (int i = 0; i < 26; i++)
            temp += (int)(Math.random() * 10);
        temp += ".";
        for (int i = 0; i < 12; i++)
            temp += (int)(Math.random() * 10);
        return new BigDecimal(temp);
    }

    //HOW TO IMPROVE?
    public static TimestampType getTimestamp()
    {
        return new TimestampType();
    }
}