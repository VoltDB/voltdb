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

package game.drivers;

import game.db.DB;
import game.gui.GUI;

public class GUIDriver
{
    private static int defaultDelayInMillis = 500;
    private static String defaultHost = "localhost";

    public static void main(String args[])
    {
        String[] hosts;
        try
        {
            hosts = args[1].split("\\s*,\\s*");
        }
        catch (Exception e)
        {
            hosts = new String[] {defaultHost};
            System.err.println("Invalid arguments or number of arguments. Reverting to default host: " + defaultHost);
        }

        DB db = new DB("GUI", hosts);
        int[] rcArray = db.getRowCol();

        int delayInMillis;
        try
        {
            delayInMillis = Integer.parseInt(args[0]);
        }
        catch (Exception e)
        {
            delayInMillis = defaultDelayInMillis;
            System.err.println("Invalid arguments or number of arguments. Reverting to default delay in milliseconds: " + defaultDelayInMillis);
        }

        GUI gui = new GUI(db, rcArray[0], rcArray[1], 5, delayInMillis);

        boolean repeat = true;
        while (repeat)
        {
            gui.repaint();
            try
            {
                Thread.sleep(gui.DELAY_MILLIS);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }
}