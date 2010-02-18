package signal;

import java.util.*;
import signal.Constants;

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

