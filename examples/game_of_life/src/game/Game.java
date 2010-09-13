/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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
package game;

import java.util.*;
import java.awt.Color;
import java.awt.Graphics;
import game.gui.*;
import game.db.*;

public class Game
{
    public final boolean DEBUG = false;

    public final int DELAY_MILLIS;

    public final int NUM_ROWS;
    public final int NUM_COLS;
    public final int NUM_CELLS;

    private boolean[] ids;
    private boolean[] future;
    private int[][] neighbors;

    private DB db;

    //initializes global variables and starts up the database
    public Game(int numRows, int numCols, int delayInMillis, String[] hosts)
    {
        NUM_ROWS = numRows;
        NUM_COLS = numCols;
        NUM_CELLS = NUM_ROWS * NUM_COLS;
        DELAY_MILLIS = delayInMillis;
        ids = new boolean[NUM_CELLS];
        future = new boolean[NUM_CELLS];
        neighbors = new int[NUM_CELLS][];
        db = new DB("Game", hosts);
        db.setRowCol(NUM_ROWS, NUM_COLS);
    }

    //randomly populates the board
    private void populateRandomly(int numLive)
    {
        double probab;
        for (int i = 0; i < NUM_CELLS; i++)
        {
            probab = Math.random();
            if (probab <= (double)numLive / (NUM_CELLS - i))
            {
                db.setIDs(i, 1);
                ids[i] = true;
                numLive--;
            }
            else
            {
                db.setIDs(i, 0);
                ids[i] = false;
            }
            neighbors[i] = findNeighborIDs(i);
        }

        if (DEBUG)
            print();
    }

    private int[] findNeighborIDs(int id)
    {
        LinkedList<Integer> neighborIDList = new LinkedList<Integer>();
        if (id / NUM_COLS > 0 && id % NUM_COLS > 0)
            neighborIDList.add(id - NUM_COLS - 1);
        if (id / NUM_COLS > 0)
            neighborIDList.add(id - NUM_COLS);
        if (id / NUM_COLS > 0 && id % NUM_COLS < NUM_COLS - 1)
            neighborIDList.add(id - NUM_COLS + 1);
        if (id % NUM_COLS > 0)
            neighborIDList.add(id - 1);
        if (id % NUM_COLS < NUM_COLS - 1)
            neighborIDList.add(id + 1);
        if (id / NUM_COLS < NUM_ROWS - 1 && id % NUM_COLS > 0)
            neighborIDList.add(id + NUM_COLS - 1);
        if (id / NUM_COLS < NUM_ROWS - 1)
            neighborIDList.add(id + NUM_COLS);
        if (id / NUM_COLS < NUM_ROWS - 1 && id % NUM_COLS < NUM_COLS - 1)
            neighborIDList.add(id + NUM_COLS + 1);

        ListIterator<Integer> li = neighborIDList.listIterator();
        int[] neighborIDs = new int[neighborIDList.size()];
        int i = 0;
        while (li.hasNext())
        {
            neighborIDs[i] = li.next();
            i++;
        }

        return neighborIDs;
    }

    //not used, but meant to automatically evolve numGenerations generations
    private void evolve(int numGenerations)
    {
        for (int i = 0; i < numGenerations; i++)
            evolve();
    }

    //advances the board by one generation
    private void evolve()
    {
        long startTime;
        long endTime;
        if (DEBUG)
            startTime = System.nanoTime();

        boolean futureBool;
        for (int i = 0; i < NUM_CELLS; i++)
        {
            futureBool = determineFuture(i);
            future[i] = futureBool;
            db.occupy(futureBool, i);
        }

        for (int i = 0; i < NUM_CELLS; i++)
            ids[i] = future[i];

        db.updateGeneration();

        if (DEBUG)
        {
            endTime = System.nanoTime();
            System.out.println("Generation took " + ((double)(endTime-startTime)/1000000000) + " seconds to calculate.");
            print();
        }
    }

    //determines whether given cell will be live or dead in next generation
    private boolean determineFuture(int id)
    {
        boolean live = ids[id];
        int[] neighborIDs = neighbors[id];
        int numLiveNeighbors = 0;

        for (int i = 0; i < neighborIDs.length; i++)
            if (ids[neighborIDs[i]])
                numLiveNeighbors++;

        if (live)
            switch (numLiveNeighbors)
            {
                case 2: case 3:
                    return true;
                default:
                    return false;
            }
        else
            switch (numLiveNeighbors)
            {
                case 3:
                    return true;
                default:
                    return false;
            }
    }

    //prints board if DEBUG is true
    private void print()
    {
        System.out.println("+----------------------------------------+");
        for (int i = 0; i < NUM_ROWS; i++)
        {
            for (int j = 0; j < NUM_COLS; j++)
                if (ids[getID(i,j)])
                    System.out.print("X ");
                else
                    System.out.print("  ");
            System.out.println();
        }
        System.out.println("+----------------------------------------+\n\n");
    }

    //returns id value based on row and column values
    private int getID(int row, int col)
    {
        return row * NUM_COLS + col;
    }

    //requests database connection termination
    private void endGame()
    {
        db.disconnect();
    }

    //requests to populate and keep evolving the board
    public void runGame(int initPopul)
    {
        populateRandomly(initPopul);
        boolean repeat = true;

        while (repeat)
        {
            evolve();
            try
            {
                Thread.sleep(DELAY_MILLIS);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        endGame();
    }
}