/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.benchmark.workloads;

import java.math.BigDecimal;
import org.voltdb.types.*;

public class RandomValues
{
    private static double probabilityOfEndingString = .01;
    private static double coinToss = .5;

    //THIS METHOD CURRENTLY TAKES A VERY SLOW APPROACH!!!
    public static String getString()
    {
        String randomString = "";

        double rand = Math.random();
        while (probabilityOfEndingString < rand)
        {
            randomString += getChar();
            rand = Math.random();
        }

        return randomString;
    }

    public static String getString(int maxLength, boolean exactLength)
    {
        String randomString = "";

        if (exactLength)
        {
            for (int i = 0; i < maxLength; i++)
                randomString += getChar();
        }
        else
        {
            double rand = Math.random();
            while ((double)randomString.length() / maxLength < rand)
            {
                randomString += getChar();
                rand = Math.random();
            }
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
            return (byte)(randomByte + Byte.MIN_VALUE);
        else
            return randomByte;
    }

    public static short getShort()
    {
        short randomShort = (short)(Math.random() * Short.MAX_VALUE);
        if (Math.random() < coinToss)
            return (short)(randomShort + Short.MIN_VALUE);
        else
            return randomShort;
    }

    public static long getLong()
    {
        long randomLong = (long)(Math.random() * Long.MAX_VALUE);
        if (Math.random() < coinToss)
            return (long)(randomLong + Long.MIN_VALUE);
        else
            return randomLong;
    }

    public static int getInt()
    {
        int randomInt = (int)(Math.random() * Integer.MAX_VALUE);
        if (Math.random() < coinToss)
            return (int)(randomInt + Integer.MIN_VALUE);
        else
            return randomInt;
    }

    public static double getDouble()
    {
        double randomDouble = (double)(Math.random() * Double.MAX_VALUE);
        if (Math.random() < coinToss)
            return (double)(randomDouble + Double.MIN_VALUE);
        else
            return randomDouble;
    }

    //UNFINISHED
    public static BigDecimal getBigDecimal()
    {
        //BigDecimal randomBigDecimal = new BigDecimal();
        return null;
    }

    //UNFINISHED
    public static TimestampType getTimestamp()
    {
        //TimestampType randomTimestampType = new TimestampType();
        return null;
    }
}