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

package tracker;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.swing.JPanel;


public class Board extends JPanel {
    private static final long serialVersionUID = 1L;

    // Add the stuff we need
    static BufferedImage worldmap;
    BufferedImage worldmapbuf;
    BufferedImage satellitepic;
    public int width;
    public int height;
    static double xscale;
    static double yscale;
    static double xorigin;
    static double yorigin;
    static ArrayList<Satellite> sat = new ArrayList<Satellite>();
    static Map<String,Color> colormap = new HashMap<String,Color>();
    static Map<String,BufferedImage> satpics = new HashMap<String,BufferedImage>();
    static int satelliteCount = -1;

    public Board() {
        init_worldmap();
        setPreferredSize(new Dimension(width,height));
    }

    @Override
    public void paintComponent(Graphics g) {
        super.paintComponent(g);

        // Override the paint command
        if (worldmap == null) { init_worldmap(); }
        g.drawImage(worldmap,  0, 0,null);

        Iterator<Satellite> i = sat.iterator();

        while(i.hasNext()) {
            Satellite s = i.next();
            BufferedImage b = satpics.get(s.country);
            g.drawImage(b,
                        (int) ((s.currentLong * xscale) + xorigin - (b.getWidth()/2)),
                        (int) ((s.currentLat * yscale) + yorigin - (b.getHeight()/2)),
                        null);
        }
    }

                // Initialize all the display elements.
    public void init_worldmap() {
        // construct the board.
        //Toolkit tk = Toolkit.getDefaultToolkit();

        //Read in the world map
        try { worldmap =  ImageIO.read(new File("worldmap.jpg"));   }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        //Read in the satellite pics
        try {
            satpics.put("usa", ImageIO.read(new File("us.gif")));
            satpics.put("france", ImageIO.read(new File("fr.gif")));
            satpics.put("brazil", ImageIO.read(new File("br.gif")));
            satpics.put("china", ImageIO.read(new File("cn.gif")));
            satpics.put("india", ImageIO.read(new File("in.gif")));

            }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        colormap.put("usa", Color.blue);
        colormap.put("france", Color.yellow);
        colormap.put("brazil", Color.green);
        colormap.put("china", Color.red);
        colormap.put("india", Color.black);

        //Set appropriate arguments
        width = worldmap.getWidth();
        height = worldmap.getHeight();
        xorigin = width/2;
        yorigin = height/2;
        xscale = width/Math.toRadians(360.0);
        yscale = height/Math.toRadians(170.0);
    }

    public static void drawmap(double a,double b,double c,double d, String country, String model) {
        final int m,n,o,p;
        m = (int) ((a * xscale) + xorigin);
        n = (int) ((b * yscale) + yorigin);
        o = (int) ((c * xscale) + xorigin);
        p = (int) ((d * yscale) + yorigin);

        Graphics g = worldmap.getGraphics();
        Graphics2D g2 = (Graphics2D) g;
        g2.setStroke(new BasicStroke(2));
        //g2.setColor(Color.yellow);
        g2.setColor(colormap.get(country));

        g2.drawLine(m,n,o,p);
    }

    public static void addSat(int id, String model, String country, double x, double y) {
        Satellite s = new Satellite(id, model, country);
        s.id = id;
        s.model = model;
        s.country = country;
        s.currentLat = y;
        s.currentLong = x;
        sat.add(id,s);
    }

    public static boolean updateSat(int id, double x, double y) {
        try {
            if (sat.get(id).id != id ) {
                System.out.println("WHAT!? id " + id + " not the same as the orbit id " + sat.get(id).id);
            }
        } catch (IndexOutOfBoundsException e) {
            return(false);
        }
        double y0 = sat.get(id).currentLat;
        double x0 = sat.get(id).currentLong;
        sat.get(id).currentLat = y;
        sat.get(id).currentLong = x;
        if ( (x > x0) && (Math.abs(y-y0)<.5 ) ) {
            drawmap(x0,y0,x,y,sat.get(id).country,sat.get(id).model);
        }
        return(true);
    }
}
