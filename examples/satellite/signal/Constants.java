package signal;

/*
 * Constants are defined to calcuate and constrict the path of the satellites.
 * For this demo, the earth is assumed to be spherical.
 */

public interface Constants {
    public static final double EARTHS_RADIUS = 6371; /* mean radius of earth (according to Wikipedia) */
    public static final double MAX_SPEED = .008;
    public static final double MIN_SPEED = .005;
    public static final double TINY_DELTA = .1;
    public static final double DELTA_FREQUENCY = .1;
}
