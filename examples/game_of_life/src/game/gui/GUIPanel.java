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
package game.gui;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.Graphics;

import javax.swing.JPanel;

import game.db.*;

public class GUIPanel extends JPanel
{
    private static final long serialVersionUID = 1L;

    private GUI gui;
    private DB db;

    private long startNanoTime = System.nanoTime();

    public GUIPanel(GUI gui, DB db)
    {
        this.db = db;
        this.gui = gui;

        setPreferredSize(new Dimension(gui.CELL_SIZE * gui.NUM_COLS, gui.CELL_SIZE * gui.NUM_ROWS + gui.OFFSET_FOR_TEXT));
    }

    @Override
    public void paintComponent(Graphics g)
    {
        super.paintComponent(g);

        long generation = db.getGeneration();
        long currTime = System.nanoTime();
        double genPerSec = (int)(100000000000. * generation / (currTime - startNanoTime)) / 100.;
        int tps = (int)(gui.NUM_COLS * gui.NUM_ROWS * genPerSec);
        g.setFont(gui.TEXT_FONT);
        g.setColor(gui.TEXT_COLOR);
        g.drawString("Generation: " + generation +
                "  Gen./second: " + genPerSec +
                "  TPS: " + addThousandSeparators(tps), gui.MARGIN, gui.MARGIN);

        boolean[] ids = db.getIDs();
        int topLeftX;
        int topLeftY;
        boolean live;

        for (int i = 0; i < ids.length; i++)
        {
            live = ids[i];
            topLeftX = getCol(i) * gui.CELL_SIZE;
            topLeftY = getRow(i) * gui.CELL_SIZE + gui.OFFSET_FOR_TEXT;
            if (live)
                g.setColor(gui.LIVE_COLOR);
            else
                g.setColor(gui.DEAD_COLOR);
            g.fillRect(topLeftX, topLeftY, gui.CELL_SIZE, gui.CELL_SIZE);
        }
    }

    private String addThousandSeparators(int num)
    {
        String temp = "" + num;
        String result = "";
        for (int i = temp.length() - 1; i >= 0; i--)
        {
            result = "" + temp.charAt(i) + result;
            if ((temp.length() - i) % 3 == 0 && i != 0)
                result = "," + result;
        }
        return result;
    }

    private int getRow(int id)
    {
        return id / gui.NUM_COLS;
    }

    private int getCol(int id)
    {
        return id % gui.NUM_COLS;
    }
}
