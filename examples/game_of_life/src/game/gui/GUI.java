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

import java.awt.Color;
import java.awt.Font;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.WindowConstants;

import game.db.*;

public class GUI extends JFrame
{
    public final int CELL_SIZE;
    public final int NUM_ROWS;
    public final int NUM_COLS;
    public final int DELAY_MILLIS;
    public final Color LIVE_COLOR = new Color((int) (Math.random() * 256), (int) (Math.random() * 256), (int) (Math.random() * 256));
    public final Color DEAD_COLOR = LIVE_COLOR.darker();
    //public final Color LIVE_COLOR = Color.WHITE;
    //public final Color DEAD_COLOR = Color.BLACK;

    public final int OFFSET_FOR_TEXT = 50;
    public final int MARGIN = 20;
    public final Color TEXT_COLOR = Color.BLUE;
    public final Font TEXT_FONT = new Font("sylfaen", Font.BOLD, 16);

    private static final long serialVersionUID = 1L;

    private JPanel panel;

    public GUI(DB db, int numRows, int numCols, int cellSize, int delayInMillis)
    {
        this.panel = new GUIPanel(this, db);
        NUM_ROWS = numRows;
        NUM_COLS = numCols;
        CELL_SIZE = cellSize;
        DELAY_MILLIS = delayInMillis;

        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        add(this.panel);
        setBounds(0,0,NUM_COLS * CELL_SIZE, NUM_ROWS * CELL_SIZE + OFFSET_FOR_TEXT);
        setVisible(true);
    }

    public void render()
    {
        panel.repaint();
    }

}