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
