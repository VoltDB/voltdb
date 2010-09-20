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

package signal;

import java.util.Random;

/*
 * Each instance of Orbit represents a single satellite.
 */
public class Orbit extends Satellite {
    public double latOffset;
    public double longOffset;
    public double peak;
    public double speed;

    Random generator = new Random();

    /*
     * When orbit is created, set up a random location, path, and speed;
     */
    Orbit(double latmin, double latmax, double longmin, double longmax) {
        peak = (generator.nextDouble() * (latmax - latmin)) + latmin;
        longOffset = ((generator.nextDouble() * (longmax - longmin)) + longmin) + (Math.PI/4);
        if (longOffset < -Math.PI) {
            longOffset = longOffset + (Math.PI * 2);
        }
        speed = (generator.nextDouble() * (Constants.MAX_SPEED - Constants.MIN_SPEED)) + Constants.MIN_SPEED;
        latOffset =0;

        // Now randomize the starting position.
        currentLong = (generator.nextDouble() * 2 * Math.PI) - Math.PI;

        // Calculate the vertical location.
        currentLat = (peak *  Math.sin(currentLong+longOffset)) + latOffset;
    }

    public void Move() {
        /*
         * See if we want to put a slight variation in the orbit.
         */
        //if (generator.nextDouble() < Constants.DELTA_FREQUENCY)
        //{
        //    longOffset = longOffset + ( ( (generator.nextInt(2)*2) - 1) * Constants.TINY_DELTA);
        //}
        // (generator.nextDouble() < Constants.DELTA_FREQUENCY)
        //{
        //    latOffset = latOffset + ( ( (generator.nextInt(2)*2) - 1) * Constants.TINY_DELTA);
        //}
        /*
         * Move the satellite....
         */
        currentLong = currentLong + speed;
        while (currentLong > Math.PI) {currentLong = currentLong - (2*Math.PI);}
        currentLat = (peak *  Math.sin(currentLong+longOffset)) + latOffset;
    }
}

