/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import java.util.Random;


public class DDLGenerator
{
	String[] datatype = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT",
			"DECIMAL", "VARCHAR", "TIMESTAMP"};

	int numOfCols;
	int idxStep;

	Random r = new Random();

	public DDLGenerator()
	{
	    numOfCols = 50;
	    idxStep = 10;
	}

	public DDLGenerator(int col, double idxPercent)
	{
	    numOfCols = col;
	    if(idxPercent < 0.01)
	    {
	        idxStep = Integer.MAX_VALUE;
	    }
	    else
	    {
	        idxStep = (int) (1 / idxPercent);
	    }
	}

	public String CreateColumn(int colNo)
	{
		int index = r.nextInt(datatype.length);
		if(datatype[index].equals("VARCHAR"))
		{
			int varcharSize = Math.abs(r.nextInt() % 1024) + 1;
			return "C" + colNo + " " + datatype[index] + "(" + varcharSize + ")";
		}
		else
		{
			return "C" + colNo + " " + datatype[index];
		}
	}

	public String CreateTable(int tableNo, String prefix)
	{
	    // number of cols range is [numOfCols / 2, numOfCols + numOfCols / 2 - 1]
		int cols = Math.abs(r.nextInt() % numOfCols) + numOfCols / 2;
		StringBuffer sb = new StringBuffer();
		sb.append("CREATE TABLE " + prefix + tableNo + " (");

		for(int i = 0; i < cols; i++)
		{
			sb.append(CreateColumn(i) + ", ");
		}

		for(int i = 0; i < cols; i += idxStep)
		{
			sb.append("UNIQUE(C" + i + "), ");
		}
		sb.append("PRIMARY KEY (C0));");

		return sb.toString();
	}

	public String DropTable(int tableNo, String prefix)
	{
	    return "DROP TABLE " + prefix + tableNo + ";";
	}
}
