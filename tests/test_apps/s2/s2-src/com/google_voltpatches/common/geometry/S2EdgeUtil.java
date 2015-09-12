/*
 * Copyright 2006 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google_voltpatches.common.geometry;

import com.google_voltpatches.common.base.Preconditions;

/**
 * This class contains various utility functions related to edges. It collects
 * together common code that is needed to implement polygonal geometry such as
 * polylines, loops, and general polygons.
 *
 */
public strictfp class S2EdgeUtil {
  /**
   * IEEE floating-point operations have a maximum error of 0.5 ULPS (units in
   * the last place). For double-precision numbers, this works out to 2**-53
   * (about 1.11e-16) times the magnitude of the result. It is possible to
   * analyze the calculation done by getIntersection() and work out the
   * worst-case rounding error. I have done a rough version of this, and my
   * estimate is that the worst case distance from the intersection point X to
   * the great circle through (a0, a1) is about 12 ULPS, or about 1.3e-15. This
   * needs to be increased by a factor of (1/0.866) to account for the
   * edgeSpliceFraction() in S2PolygonBuilder. Note that the maximum error
   * measured by the unittest in 1,000,000 trials is less than 3e-16.
   */
  public static final S1Angle DEFAULT_INTERSECTION_TOLERANCE = S1Angle.radians(1.5e-15);

  /**
   * This class allows a vertex chain v0, v1, v2, ... to be efficiently tested
   * for intersection with a given fixed edge AB.
   */
  public static class EdgeCrosser {
    // The fields below are all constant.

    private final S2Point a;
    private final S2Point b;
    private final S2Point aCrossB;

    // The fields below are updated for each vertex in the chain.

    // Previous vertex in the vertex chain.
    private S2Point c;
    // The orientation of the triangle ACB.
    private int acb;

    /**
     * AB is the given fixed edge, and C is the first vertex of the vertex
     * chain. All parameters must point to fixed storage that persists for the
     * lifetime of the EdgeCrosser object.
     */
    public EdgeCrosser(S2Point a, S2Point b, S2Point c) {
      this.a = a;
      this.b = b;
      this.aCrossB = S2Point.crossProd(a, b);
      restartAt(c);
    }

    /**
     * Call this function when your chain 'jumps' to a new place.
     */
    public void restartAt(S2Point c) {
      this.c = c;
      acb = -S2.robustCCW(a, b, c, aCrossB);
    }

    /**
     * This method is equivalent to calling the S2EdgeUtil.robustCrossing()
     * function (defined below) on the edges AB and CD. It returns +1 if there
     * is a crossing, -1 if there is no crossing, and 0 if two points from
     * different edges are the same. Returns 0 or -1 if either edge is
     * degenerate. As a side effect, it saves vertex D to be used as the next
     * vertex C.
     */
    public int robustCrossing(S2Point d) {
      // For there to be an edge crossing, the triangles ACB, CBD, BDA, DAC must
      // all be oriented the same way (CW or CCW). We keep the orientation
      // of ACB as part of our state. When each new point D arrives, we
      // compute the orientation of BDA and check whether it matches ACB.
      // This checks whether the points C and D are on opposite sides of the
      // great circle through AB.

      // Recall that robustCCW is invariant with respect to rotating its
      // arguments, i.e. ABC has the same orientation as BDA.
      int bda = S2.robustCCW(a, b, d, aCrossB);
      int result;

      if (bda == -acb && bda != 0) {
        // Most common case -- triangles have opposite orientations.
        result = -1;
      } else if ((bda & acb) == 0) {
        // At least one value is zero -- two vertices are identical.
        result = 0;
      } else {
        // assert (bda == acb && bda != 0);
        result = robustCrossingInternal(d); // Slow path.
      }
      // Now save the current vertex D as the next vertex C, and also save the
      // orientation of the new triangle ACB (which is opposite to the current
      // triangle BDA).
      c = d;
      acb = -bda;
      return result;
    }

    /**
     * This method is equivalent to the S2EdgeUtil.edgeOrVertexCrossing() method
     * defined below. It is similar to robustCrossing, but handles cases where
     * two vertices are identical in a way that makes it easy to implement
     * point-in-polygon containment tests.
     */
    public boolean edgeOrVertexCrossing(S2Point d) {
      // We need to copy c since it is clobbered by robustCrossing().
      S2Point c2 = new S2Point(c.get(0), c.get(1), c.get(2));

      int crossing = robustCrossing(d);
      if (crossing < 0) {
        return false;
      }
      if (crossing > 0) {
        return true;
      }

      return vertexCrossing(a, b, c2, d);
    }

    /**
     * This function handles the "slow path" of robustCrossing().
     */
    private int robustCrossingInternal(S2Point d) {
      // ACB and BDA have the appropriate orientations, so now we check the
      // triangles CBD and DAC.
      S2Point cCrossD = S2Point.crossProd(c, d);
      int cbd = -S2.robustCCW(c, d, b, cCrossD);
      if (cbd != acb) {
        return -1;
      }

      int dac = S2.robustCCW(c, d, a, cCrossD);
      return (dac == acb) ? 1 : -1;
    }
  }

  /**
   * This class computes a bounding rectangle that contains all edges defined by
   * a vertex chain v0, v1, v2, ... All vertices must be unit length. Note that
   * the bounding rectangle of an edge can be larger than the bounding rectangle
   * of its endpoints, e.g. consider an edge that passes through the north pole.
   */
  public static class RectBounder {
    // The previous vertex in the chain.
    private S2Point a;

    // The corresponding latitude-longitude.
    private S2LatLng aLatLng;

    // The current bounding rectangle.
    private S2LatLngRect bound;

    public RectBounder() {
      this.bound = S2LatLngRect.empty();
    }

    /**
     * This method is called to add each vertex to the chain. 'b' must point to
     * fixed storage that persists for the lifetime of the RectBounder.
     */
    public void addPoint(S2Point b) {
      // assert (S2.isUnitLength(b));

      S2LatLng bLatLng = new S2LatLng(b);

      if (bound.isEmpty()) {
        bound = bound.addPoint(bLatLng);
      } else {
        // We can't just call bound.addPoint(bLatLng) here, since we need to
        // ensure that all the longitudes between "a" and "b" are included.
        bound = bound.union(S2LatLngRect.fromPointPair(aLatLng, bLatLng));

        // Check whether the min/max latitude occurs in the edge interior.
        // We find the normal to the plane containing AB, and then a vector
        // "dir" in this plane that also passes through the equator. We use
        // RobustCrossProd to ensure that the edge normal is accurate even
        // when the two points are very close together.
        S2Point aCrossB = S2.robustCrossProd(a, b);
        S2Point dir = S2Point.crossProd(aCrossB, new S2Point(0, 0, 1));
        double da = dir.dotProd(a);
        double db = dir.dotProd(b);

        if (da * db < 0) {
          // Minimum/maximum latitude occurs in the edge interior. This affects
          // the latitude bounds but not the longitude bounds.
          double absLat = Math.acos(Math.abs(aCrossB.get(2) / aCrossB.norm()));
          R1Interval lat = bound.lat();
          if (da < 0) {
            // It's possible that absLat < lat.lo() due to numerical errors.
            lat = new R1Interval(lat.lo(), Math.max(absLat, bound.lat().hi()));
          } else {
            lat = new R1Interval(Math.min(-absLat, bound.lat().lo()), lat.hi());
          }
          bound = new S2LatLngRect(lat, bound.lng());
        }
      }
      a = b;
      aLatLng = bLatLng;
    }

    /**
     * Return the bounding rectangle of the edge chain that connects the
     * vertices defined so far.
     */
    public S2LatLngRect getBound() {
      return bound;
    }

  }

  /**
   * The purpose of this class is to find edges that intersect a given XYZ
   * bounding box. It can be used as an efficient rejection test when attempting to
   * find edges that intersect a given region. It accepts a vertex chain v0, v1,
   * v2, ... and returns a boolean value indicating whether each edge intersects
   * the specified bounding box.
   *
   * We use XYZ intervals instead of something like longitude intervals because
   * it is cheap to collect from S2Point lists and any slicing strategy should
   * give essentially equivalent results.  See S2Loop for an example of use.
   */
  public static class XYZPruner {
    private S2Point lastVertex;

    // The region to be tested against.
    private boolean boundSet;
    private double xmin;
    private double ymin;
    private double zmin;
    private double xmax;
    private double ymax;
    private double zmax;
    private double maxDeformation;

    public XYZPruner() {
      boundSet = false;
    }

    /**
     * Accumulate a bounding rectangle from provided edges.
     *
     * @param from start of edge
     * @param to end of edge.
     */
    public void addEdgeToBounds(S2Point from, S2Point to) {
      if (!boundSet) {
        boundSet = true;
        xmin = xmax = from.x;
        ymin = ymax = from.y;
        zmin = zmax = from.z;
      }
      xmin = Math.min(xmin, Math.min(to.x, from.x));
      ymin = Math.min(ymin, Math.min(to.y, from.y));
      zmin = Math.min(zmin, Math.min(to.z, from.z));
      xmax = Math.max(xmax, Math.max(to.x, from.x));
      ymax = Math.max(ymax, Math.max(to.y, from.y));
      zmax = Math.max(zmax, Math.max(to.z, from.z));

      // Because our arcs are really geodesics on the surface of the earth
      // an edge can have intermediate points outside the xyz bounds implicit
      // in the end points.  Based on the length of the arc we compute a
      // generous bound for the maximum amount of deformation.  For small edges
      // it will be very small but for some large arcs (ie. from (1N,90W) to
      // (1N,90E) the path can be wildly deformed.  I did a bunch of
      // experiments with geodesics to get safe bounds for the deformation.
      double approxArcLen =
          Math.abs(from.x - to.x) + Math.abs(from.y - to.y) + Math.abs(from.z - to.z);
      if (approxArcLen < 0.025) { // less than 2 degrees
        maxDeformation = Math.max(maxDeformation, approxArcLen * 0.0025);
      } else if (approxArcLen < 1.0) { // less than 90 degrees
        maxDeformation = Math.max(maxDeformation, approxArcLen * 0.11);
      } else {
        maxDeformation = approxArcLen * 0.5;
      }
    }

    public void setFirstIntersectPoint(S2Point v0) {
      xmin = xmin - maxDeformation;
      ymin = ymin - maxDeformation;
      zmin = zmin - maxDeformation;
      xmax = xmax + maxDeformation;
      ymax = ymax + maxDeformation;
      zmax = zmax + maxDeformation;
      this.lastVertex = v0;
    }

    /**
     * Returns true if the edge going from the last point to this point passes
     * through the pruner bounding box, otherwise returns false.  So the
     * method returns false if we are certain there is no intersection, but it
     * may return true when there turns out to be no intersection.
     */
    public boolean intersects(S2Point v1) {
      boolean result = true;

      if ((v1.x < xmin && lastVertex.x < xmin) || (v1.x > xmax && lastVertex.x > xmax)) {
        result = false;
      } else if ((v1.y < ymin && lastVertex.y < ymin) || (v1.y > ymax && lastVertex.y > ymax)) {
        result = false;
      } else if ((v1.z < zmin && lastVertex.z < zmin) || (v1.z > zmax && lastVertex.z > zmax)) {
        result = false;
      }

      lastVertex = v1;
      return result;
    }
  }

  /**
   * The purpose of this class is to find edges that intersect a given longitude
   * interval. It can be used as an efficient rejection test when attempting to
   * find edges that intersect a given region. It accepts a vertex chain v0, v1,
   * v2, ... and returns a boolean value indicating whether each edge intersects
   * the specified longitude interval.
   *
   * This class is not currently used as the XYZPruner is preferred for
   * S2Loop, but this should be usable in similar circumstances.  Be wary
   * of the cost of atan2() in conversions from S2Point to longitude!
   */
  public static class LongitudePruner {
    // The interval to be tested against.
    private S1Interval interval;

    // The longitude of the next v0.
    private double lng0;

    /**
     *'interval' is the longitude interval to be tested against, and 'v0' is
     * the first vertex of edge chain.
     */
    public LongitudePruner(S1Interval interval, S2Point v0) {
      this.interval = interval;
      this.lng0 = S2LatLng.longitude(v0).radians();
    }

    /**
     * Returns true if the edge (v0, v1) intersects the given longitude
     * interval, and then saves 'v1' to be used as the next 'v0'.
     */
    public boolean intersects(S2Point v1) {
      double lng1 = S2LatLng.longitude(v1).radians();
      boolean result = interval.intersects(S1Interval.fromPointPair(lng0, lng1));
      lng0 = lng1;
      return result;
    }
  }

  /**
   * A wedge relation's test method accepts two edge chains A=(a0,a1,a2) and
   * B=(b0,b1,b2) where a1==b1, and returns either -1, 0, or 1 to indicate the
   * relationship between the region to the left of A and the region to the left
   * of B. Wedge relations are used to determine the local relationship between
   * two polygons that share a common vertex.
   *
   *  All wedge relations require that a0 != a2 and b0 != b2. Other degenerate
   * cases (such as a0 == b2) are handled as expected. The parameter "ab1"
   * denotes the common vertex a1 == b1.
   */
  public interface WedgeRelation {
    int test(S2Point a0, S2Point ab1, S2Point a2, S2Point b0, S2Point b2);
  }

  public static class WedgeContains implements WedgeRelation {
    /**
     * Given two edge chains (see WedgeRelation above), this function returns +1
     * if the region to the left of A contains the region to the left of B, and
     * 0 otherwise.
     */
    @Override
    public int test(S2Point a0, S2Point ab1, S2Point a2, S2Point b0, S2Point b2) {
      // For A to contain B (where each loop interior is defined to be its left
      // side), the CCW edge order around ab1 must be a2 b2 b0 a0. We split
      // this test into two parts that test three vertices each.
      return S2.orderedCCW(a2, b2, b0, ab1) && S2.orderedCCW(b0, a0, a2, ab1) ? 1 : 0;
    }
  }

  public static class WedgeIntersects implements WedgeRelation {
    /**
     * Given two edge chains (see WedgeRelation above), this function returns -1
     * if the region to the left of A intersects the region to the left of B,
     * and 0 otherwise. Note that regions are defined such that points along a
     * boundary are contained by one side or the other, not both. So for
     * example, if A,B,C are distinct points ordered CCW around a vertex O, then
     * the wedges BOA, AOC, and COB do not intersect.
     */
    @Override
    public int test(S2Point a0, S2Point ab1, S2Point a2, S2Point b0, S2Point b2) {
      // For A not to intersect B (where each loop interior is defined to be
      // its left side), the CCW edge order around ab1 must be a0 b2 b0 a2.
      // Note that it's important to write these conditions as negatives
      // (!OrderedCCW(a,b,c,o) rather than Ordered(c,b,a,o)) to get correct
      // results when two vertices are the same.
      return (S2.orderedCCW(a0, b2, b0, ab1) && S2.orderedCCW(b0, a2, a0, ab1) ? 0 : -1);
    }
  }

  public static class WedgeContainsOrIntersects implements WedgeRelation {
    /**
     * Given two edge chains (see WedgeRelation above), this function returns +1
     * if A contains B, 0 if A and B are disjoint, and -1 if A intersects but
     * does not contain B.
     */
    @Override
    public int test(S2Point a0, S2Point ab1, S2Point a2, S2Point b0, S2Point b2) {
      // This is similar to WedgeContainsOrCrosses, except that we want to
      // distinguish cases (1) [A contains B], (3) [A and B are disjoint],
      // and (2,4,5,6) [A intersects but does not contain B].

      if (S2.orderedCCW(a0, a2, b2, ab1)) {
        // We are in case 1, 5, or 6, or case 2 if a2 == b2.
        return S2.orderedCCW(b2, b0, a0, ab1) ? 1 : -1; // Case 1 vs. 2,5,6.
      }
      // We are in cases 2, 3, or 4.
      if (!S2.orderedCCW(a2, b0, b2, ab1)) {
        return 0; // Case 3.
      }

      // We are in case 2 or 4, or case 3 if a2 == b0.
      return (a2.equals(b0)) ? 0 : -1; // Case 3 vs. 2,4.
    }
  }

  public static class WedgeContainsOrCrosses implements WedgeRelation {
    /**
     * Given two edge chains (see WedgeRelation above), this function returns +1
     * if A contains B, 0 if B contains A or the two wedges do not intersect,
     * and -1 if the edge chains A and B cross each other (i.e. if A intersects
     * both the interior and exterior of the region to the left of B). In
     * degenerate cases where more than one of these conditions is satisfied,
     * the maximum possible result is returned. For example, if A == B then the
     * result is +1.
     */
    @Override
    public int test(S2Point a0, S2Point ab1, S2Point a2, S2Point b0, S2Point b2) {
      // There are 6 possible edge orderings at a shared vertex (all
      // of these orderings are circular, i.e. abcd == bcda):
      //
      // (1) a2 b2 b0 a0: A contains B
      // (2) a2 a0 b0 b2: B contains A
      // (3) a2 a0 b2 b0: A and B are disjoint
      // (4) a2 b0 a0 b2: A and B intersect in one wedge
      // (5) a2 b2 a0 b0: A and B intersect in one wedge
      // (6) a2 b0 b2 a0: A and B intersect in two wedges
      //
      // In cases (4-6), the boundaries of A and B cross (i.e. the boundary
      // of A intersects the interior and exterior of B and vice versa).
      // Thus we want to distinguish cases (1), (2-3), and (4-6).
      //
      // Note that the vertices may satisfy more than one of the edge
      // orderings above if two or more vertices are the same. The tests
      // below are written so that we take the most favorable
      // interpretation, i.e. preferring (1) over (2-3) over (4-6). In
      // particular note that if orderedCCW(a,b,c,o) returns true, it may be
      // possible that orderedCCW(c,b,a,o) is also true (if a == b or b == c).

      if (S2.orderedCCW(a0, a2, b2, ab1)) {
        // The cases with this vertex ordering are 1, 5, and 6,
        // although case 2 is also possible if a2 == b2.
        if (S2.orderedCCW(b2, b0, a0, ab1)) {
          return 1; // Case 1 (A contains B)
        }

        // We are in case 5 or 6, or case 2 if a2 == b2.
        return (a2.equals(b2)) ? 0 : -1; // Case 2 vs. 5,6.
      }
      // We are in case 2, 3, or 4.
      return S2.orderedCCW(a0, b0, a2, ab1) ? 0 : -1; // Case 2,3 vs. 4.
    }
  }

  /**
   * Return true if edge AB crosses CD at a point that is interior to both
   * edges. Properties:
   *
   *  (1) simpleCrossing(b,a,c,d) == simpleCrossing(a,b,c,d) (2)
   * simpleCrossing(c,d,a,b) == simpleCrossing(a,b,c,d)
   */
  public static boolean simpleCrossing(S2Point a, S2Point b, S2Point c, S2Point d) {
    // We compute simpleCCW() for triangles ACB, CBD, BDA, and DAC. All
    // of these triangles need to have the same orientation (CW or CCW)
    // for an intersection to exist. Note that this is slightly more
    // restrictive than the corresponding definition for planar edges,
    // since we need to exclude pairs of line segments that would
    // otherwise "intersect" by crossing two antipodal points.

    S2Point ab = S2Point.crossProd(a, b);
    double acb = -(ab.dotProd(c));
    double bda = ab.dotProd(d);
    if (acb * bda <= 0) {
      return false;
    }

    S2Point cd = S2Point.crossProd(c, d);
    double cbd = -(cd.dotProd(b));
    double dac = cd.dotProd(a);
    return (acb * cbd > 0) && (acb * dac > 0);
  }

  /**
   * Like SimpleCrossing, except that points that lie exactly on a line are
   * arbitrarily classified as being on one side or the other (according to the
   * rules of S2.robustCCW). It returns +1 if there is a crossing, -1 if there
   * is no crossing, and 0 if any two vertices from different edges are the
   * same. Returns 0 or -1 if either edge is degenerate. Properties of
   * robustCrossing:
   *
   *  (1) robustCrossing(b,a,c,d) == robustCrossing(a,b,c,d) (2)
   * robustCrossing(c,d,a,b) == robustCrossing(a,b,c,d) (3)
   * robustCrossing(a,b,c,d) == 0 if a==c, a==d, b==c, b==d (3)
   * robustCrossing(a,b,c,d) <= 0 if a==b or c==d
   *
   *  Note that if you want to check an edge against a *chain* of other edges,
   * it is much more efficient to use an EdgeCrosser (above).
   */
  public static int robustCrossing(S2Point a, S2Point b, S2Point c, S2Point d) {
    // For there to be a crossing, the triangles ACB, CBD, BDA, DAC must
    // all have the same orientation (clockwise or counterclockwise).
    //
    // First we compute the orientation of ACB and BDA. We permute the
    // arguments to robustCCW so that we can reuse the cross-product of A and B.
    // Recall that when the arguments to robustCCW are permuted, the sign of the
    // result changes according to the sign of the permutation. Thus ACB and
    // ABC are oppositely oriented, while BDA and ABD are the same.
    S2Point aCrossB = S2Point.crossProd(a, b);
    int acb = -S2.robustCCW(a, b, c, aCrossB);
    int bda = S2.robustCCW(a, b, d, aCrossB);

    // If any two vertices are the same, the result is degenerate.
    if ((bda & acb) == 0) {
      return 0;
    }

    // If ABC and BDA have opposite orientations (the most common case),
    // there is no crossing.
    if (bda != acb) {
      return -1;
    }

    // Otherwise we compute the orientations of CBD and DAC, and check whether
    // their orientations are compatible with the other two triangles.
    S2Point cCrossD = S2Point.crossProd(c, d);
    int cbd = -S2.robustCCW(c, d, b, cCrossD);
    if (cbd != acb) {
      return -1;
    }

    int dac = S2.robustCCW(c, d, a, cCrossD);
    return (dac == acb) ? 1 : -1;
  }

  /**
   * Given two edges AB and CD where at least two vertices are identical (i.e.
   * robustCrossing(a,b,c,d) == 0), this function defines whether the two edges
   * "cross" in a such a way that point-in-polygon containment tests can be
   * implemented by counting the number of edge crossings. The basic rule is
   * that a "crossing" occurs if AB is encountered after CD during a CCW sweep
   * around the shared vertex starting from a fixed reference point.
   *
   *  Note that according to this rule, if AB crosses CD then in general CD does
   * not cross AB. However, this leads to the correct result when counting
   * polygon edge crossings. For example, suppose that A,B,C are three
   * consecutive vertices of a CCW polygon. If we now consider the edge
   * crossings of a segment BP as P sweeps around B, the crossing number changes
   * parity exactly when BP crosses BA or BC.
   *
   *  Useful properties of VertexCrossing (VC):
   *
   *  (1) VC(a,a,c,d) == VC(a,b,c,c) == false (2) VC(a,b,a,b) == VC(a,b,b,a) ==
   * true (3) VC(a,b,c,d) == VC(a,b,d,c) == VC(b,a,c,d) == VC(b,a,d,c) (3) If
   * exactly one of a,b equals one of c,d, then exactly one of VC(a,b,c,d) and
   * VC(c,d,a,b) is true
   *
   * It is an error to call this method with 4 distinct vertices.
   */
  public static boolean vertexCrossing(S2Point a, S2Point b, S2Point c, S2Point d) {
    // If A == B or C == D there is no intersection. We need to check this
    // case first in case 3 or more input points are identical.
    if (a.equals(b) || c.equals(d)) {
      return false;
    }

    // If any other pair of vertices is equal, there is a crossing if and only
    // if orderedCCW() indicates that the edge AB is further CCW around the
    // shared vertex than the edge CD.
    if (a.equals(d)) {
      return S2.orderedCCW(S2.ortho(a), c, b, a);
    }
    if (b.equals(c)) {
      return S2.orderedCCW(S2.ortho(b), d, a, b);
    }
    if (a.equals(c)) {
      return S2.orderedCCW(S2.ortho(a), d, b, a);
    }
    if (b.equals(d)) {
      return S2.orderedCCW(S2.ortho(b), c, a, b);
    }

    // assert (false);
    return false;
  }

  /**
   * A convenience function that calls robustCrossing() to handle cases where
   * all four vertices are distinct, and VertexCrossing() to handle cases where
   * two or more vertices are the same. This defines a crossing function such
   * that point-in-polygon containment tests can be implemented by simply
   * counting edge crossings.
   */
  public static boolean edgeOrVertexCrossing(S2Point a, S2Point b, S2Point c, S2Point d) {
    int crossing = robustCrossing(a, b, c, d);
    if (crossing < 0) {
      return false;
    }
    if (crossing > 0) {
      return true;
    }
    return vertexCrossing(a, b, c, d);
  }

  static class CloserResult {
    private double dmin2;
    private S2Point vmin;

    public double getDmin2() {
      return dmin2;
    }

    public S2Point getVmin() {
      return vmin;
    }

    public CloserResult(double dmin2, S2Point vmin) {
      this.dmin2 = dmin2;
      this.vmin = vmin;
    }

    public void replaceIfCloser(S2Point x, S2Point y) {
      // If the squared distance from x to y is less than dmin2, then replace
      // vmin by y and update dmin2 accordingly.
      double d2 = S2Point.minus(x, y).norm2();
      if (d2 < dmin2 || (d2 == dmin2 && y.lessThan(vmin))) {
        dmin2 = d2;
        vmin = y;
      }
    }
  }

  /*
   * Given two edges AB and CD such that robustCrossing() is true, return their
   * intersection point. Useful properties of getIntersection (GI):
   *
   * (1) GI(b,a,c,d) == GI(a,b,d,c) == GI(a,b,c,d) (2) GI(c,d,a,b) ==
   * GI(a,b,c,d)
   *
   * The returned intersection point X is guaranteed to be close to the edges AB
   * and CD, but if the edges intersect at a very small angle then X may not be
   * close to the true mathematical intersection point P. See the description of
   * "DEFAULT_INTERSECTION_TOLERANCE" below for details.
   */
  public static S2Point getIntersection(S2Point a0, S2Point a1, S2Point b0, S2Point b1) {
    Preconditions.checkArgument(robustCrossing(a0, a1, b0, b1) > 0,
        "Input edges a0a1 and b0b1 muct have a true robustCrossing.");

    // We use robustCrossProd() to get accurate results even when two endpoints
    // are close together, or when the two line segments are nearly parallel.
    S2Point aNorm = S2Point.normalize(S2.robustCrossProd(a0, a1));
    S2Point bNorm = S2Point.normalize(S2.robustCrossProd(b0, b1));
    S2Point x = S2Point.normalize(S2.robustCrossProd(aNorm, bNorm));

    // Make sure the intersection point is on the correct side of the sphere.
    // Since all vertices are unit length, and edges are less than 180 degrees,
    // (a0 + a1) and (b0 + b1) both have positive dot product with the
    // intersection point. We use the sum of all vertices to make sure that the
    // result is unchanged when the edges are reversed or exchanged.
    if (x.dotProd(S2Point.add(S2Point.add(a0, a1), S2Point.add(b0, b1))) < 0) {
      x = S2Point.neg(x);
    }

    // The calculation above is sufficient to ensure that "x" is within
    // DEFAULT_INTERSECTION_TOLERANCE of the great circles through (a0,a1) and
    // (b0,b1).
    // However, if these two great circles are very close to parallel, it is
    // possible that "x" does not lie between the endpoints of the given line
    // segments. In other words, "x" might be on the great circle through
    // (a0,a1) but outside the range covered by (a0,a1). In this case we do
    // additional clipping to ensure that it does.

    if (S2.orderedCCW(a0, x, a1, aNorm) && S2.orderedCCW(b0, x, b1, bNorm)) {
      return x;
    }

    // Find the acceptable endpoint closest to x and return it. An endpoint is
    // acceptable if it lies between the endpoints of the other line segment.
    CloserResult r = new CloserResult(10, x);
    if (S2.orderedCCW(b0, a0, b1, bNorm)) {
      r.replaceIfCloser(x, a0);
    }
    if (S2.orderedCCW(b0, a1, b1, bNorm)) {
      r.replaceIfCloser(x, a1);
    }
    if (S2.orderedCCW(a0, b0, a1, aNorm)) {
      r.replaceIfCloser(x, b0);
    }
    if (S2.orderedCCW(a0, b1, a1, aNorm)) {
      r.replaceIfCloser(x, b1);
    }
    return r.getVmin();
  }

  /**
   * Given a point X and an edge AB, return the distance ratio AX / (AX + BX).
   * If X happens to be on the line segment AB, this is the fraction "t" such
   * that X == Interpolate(A, B, t). Requires that A and B are distinct.
   */
  public static double getDistanceFraction(S2Point x, S2Point a0, S2Point a1) {
    Preconditions.checkArgument(!a0.equals(a1));
    double d0 = x.angle(a0);
    double d1 = x.angle(a1);
    return d0 / (d0 + d1);
  }

  /**
   * Return the minimum distance from X to any point on the edge AB. The result
   * is very accurate for small distances but may have some numerical error if
   * the distance is large (approximately Pi/2 or greater). The case A == B is
   * handled correctly. Note: x, a and b must be of unit length. Throws
   * IllegalArgumentException if this is not the case.
   */
  public static S1Angle getDistance(S2Point x, S2Point a, S2Point b) {
    return getDistance(x, a, b, S2.robustCrossProd(a, b));
  }

  /**
   * A slightly more efficient version of getDistance() where the cross product
   * of the two endpoints has been precomputed. The cross product does not need
   * to be normalized, but should be computed using S2.robustCrossProd() for the
   * most accurate results.
   */
  public static S1Angle getDistance(S2Point x, S2Point a, S2Point b, S2Point aCrossB) {
    Preconditions.checkArgument(S2.isUnitLength(x));
    Preconditions.checkArgument(S2.isUnitLength(a));
    Preconditions.checkArgument(S2.isUnitLength(b));

    // There are three cases. If X is located in the spherical wedge defined by
    // A, B, and the axis A x B, then the closest point is on the segment AB.
    // Otherwise the closest point is either A or B; the dividing line between
    // these two cases is the great circle passing through (A x B) and the
    // midpoint of AB.

    if (S2.simpleCCW(aCrossB, a, x) && S2.simpleCCW(x, b, aCrossB)) {
      // The closest point to X lies on the segment AB. We compute the distance
      // to the corresponding great circle. The result is accurate for small
      // distances but not necessarily for large distances (approaching Pi/2).

      double sinDist = Math.abs(x.dotProd(aCrossB)) / aCrossB.norm();
      return S1Angle.radians(Math.asin(Math.min(1.0, sinDist)));
    }

    // Otherwise, the closest point is either A or B. The cheapest method is
    // just to compute the minimum of the two linear (as opposed to spherical)
    // distances and convert the result to an angle. Again, this method is
    // accurate for small but not large distances (approaching Pi).

    double linearDist2 = Math.min(S2Point.minus(x, a).norm2(), S2Point.minus(x, b).norm2());
    return S1Angle.radians(2 * Math.asin(Math.min(1.0, 0.5 * Math.sqrt(linearDist2))));
  }

  /**
   * Returns the point on edge AB closest to X. x, a and b must be of unit
   * length. Throws IllegalArgumentException if this is not the case.
   *
   */
  public static S2Point getClosestPoint(S2Point x, S2Point a, S2Point b) {
    Preconditions.checkArgument(S2.isUnitLength(x));
    Preconditions.checkArgument(S2.isUnitLength(a));
    Preconditions.checkArgument(S2.isUnitLength(b));

    S2Point crossProd = S2.robustCrossProd(a, b);
    // Find the closest point to X along the great circle through AB.
    S2Point p = S2Point.minus(x, S2Point.mul(crossProd, x.dotProd(crossProd) / crossProd.norm2()));

    // If p is on the edge AB, then it's the closest point.
    if (S2.simpleCCW(crossProd, a, p) && S2.simpleCCW(p, b, crossProd)) {
      return S2Point.normalize(p);
    }
    // Otherwise, the closest point is either A or B.
    return S2Point.minus(x, a).norm2() <= S2Point.minus(x, b).norm2() ? a : b;
  }

  /** Constructor is private so that this class is never instantiated. */
  private S2EdgeUtil() {
  }
}
