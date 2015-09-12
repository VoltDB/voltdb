/*
 * Copyright 2005 Google Inc.
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


/**
 * An S2Cell is an S2Region object that represents a cell. Unlike S2CellIds, it
 * supports efficient containment and intersection tests. However, it is also a
 * more expensive representation.
 *
 */

public final strictfp class S2Cell implements S2Region {

  private static final int MAX_CELL_SIZE = 1 << S2CellId.MAX_LEVEL;

  byte face;
  byte level;
  byte orientation;
  S2CellId cellId;
  double[][] uv = new double[2][2];

  /**
   * Default constructor used only internally.
   */
  S2Cell() {
  }

  /**
   * An S2Cell always corresponds to a particular S2CellId. The other
   * constructors are just convenience methods.
   */
  public S2Cell(S2CellId id) {
    init(id);
  }

  // This is a static method in order to provide named parameters.
  public static S2Cell fromFacePosLevel(int face, byte pos, int level) {
    return new S2Cell(S2CellId.fromFacePosLevel(face, pos, level));
  }

  // Convenience methods.
  public S2Cell(S2Point p) {
    init(S2CellId.fromPoint(p));
  }

  public S2Cell(S2LatLng ll) {
    init(S2CellId.fromLatLng(ll));
  }


  public S2CellId id() {
    return cellId;
  }

  public int face() {
    return face;
  }

  public byte level() {
    return level;
  }

  public byte orientation() {
    return orientation;
  }

  public boolean isLeaf() {
    return level == S2CellId.MAX_LEVEL;
  }

  public S2Point getVertex(int k) {
    return S2Point.normalize(getVertexRaw(k));
  }

  /**
   * Return the k-th vertex of the cell (k = 0,1,2,3). Vertices are returned in
   * CCW order. The points returned by GetVertexRaw are not necessarily unit
   * length.
   */
  public S2Point getVertexRaw(int k) {
    // Vertices are returned in the order SW, SE, NE, NW.
    return S2Projections.faceUvToXyz(face, uv[0][(k >> 1) ^ (k & 1)], uv[1][k >> 1]);
  }

  public S2Point getEdge(int k) {
    return S2Point.normalize(getEdgeRaw(k));
  }

  public S2Point getEdgeRaw(int k) {
    switch (k) {
      case 0:
        return S2Projections.getVNorm(face, uv[1][0]); // South
      case 1:
        return S2Projections.getUNorm(face, uv[0][1]); // East
      case 2:
        return S2Point.neg(S2Projections.getVNorm(face, uv[1][1])); // North
      default:
        return S2Point.neg(S2Projections.getUNorm(face, uv[0][0])); // West
    }
  }

  /**
   * Return the inward-facing normal of the great circle passing through the
   * edge from vertex k to vertex k+1 (mod 4). The normals returned by
   * GetEdgeRaw are not necessarily unit length.
   *
   *  If this is not a leaf cell, set children[0..3] to the four children of
   * this cell (in traversal order) and return true. Otherwise returns false.
   * This method is equivalent to the following:
   *
   *  for (pos=0, id=child_begin(); id != child_end(); id = id.next(), ++pos)
   * children[i] = S2Cell(id);
   *
   * except that it is more than two times faster.
   */
  public boolean subdivide(S2Cell children[]) {
    // This function is equivalent to just iterating over the child cell ids
    // and calling the S2Cell constructor, but it is about 2.5 times faster.

    if (cellId.isLeaf()) {
      return false;
    }

    // Compute the cell midpoint in uv-space.
    R2Vector uvMid = getCenterUV();

    // Create four children with the appropriate bounds.
    S2CellId id = cellId.childBegin();
    for (int pos = 0; pos < 4; ++pos, id = id.next()) {
      S2Cell child = children[pos];
      child.face = face;
      child.level = (byte) (level + 1);
      child.orientation = (byte) (orientation ^ S2.posToOrientation(pos));
      child.cellId = id;
      int ij = S2.posToIJ(orientation, pos);
      for (int d = 0; d < 2; ++d) {
        // The dimension 0 index (i/u) is in bit 1 of ij.
        int m = 1 - ((ij >> (1 - d)) & 1);
        child.uv[d][m] = uvMid.get(d);
        child.uv[d][1 - m] = uv[d][1 - m];
      }
    }
    return true;
  }

  /**
   * Return the direction vector corresponding to the center in (s,t)-space of
   * the given cell. This is the point at which the cell is divided into four
   * subcells; it is not necessarily the centroid of the cell in (u,v)-space or
   * (x,y,z)-space. The point returned by GetCenterRaw is not necessarily unit
   * length.
   */
  public S2Point getCenter() {
    return S2Point.normalize(getCenterRaw());
  }

  public S2Point getCenterRaw() {
    return cellId.toPointRaw();
  }

  /**
   * Return the center of the cell in (u,v) coordinates (see {@code
   * S2Projections}). Note that the center of the cell is defined as the point
   * at which it is recursively subdivided into four children; in general, it is
   * not at the midpoint of the (u,v) rectangle covered by the cell
   */
  public R2Vector getCenterUV() {
    MutableInteger i = new MutableInteger(0);
    MutableInteger j = new MutableInteger(0);
    cellId.toFaceIJOrientation(i, j, null);
    int cellSize = 1 << (S2CellId.MAX_LEVEL - level);

    // TODO(dbeaumont): Figure out a better naming of the variables here (and elsewhere).
    int si = (i.intValue() & -cellSize) * 2 + cellSize - MAX_CELL_SIZE;
    double x = S2Projections.stToUV((1.0 / MAX_CELL_SIZE) * si);

    int sj = (j.intValue() & -cellSize) * 2 + cellSize - MAX_CELL_SIZE;
    double y = S2Projections.stToUV((1.0 / MAX_CELL_SIZE) * sj);

    return new R2Vector(x, y);
  }

  /**
   * Return the average area for cells at the given level.
   */
  public static double averageArea(int level) {
    return S2Projections.AVG_AREA.getValue(level);
  }

  /**
   * Return the average area of cells at this level. This is accurate to within
   * a factor of 1.7 (for S2_QUADRATIC_PROJECTION) and is extremely cheap to
   * compute.
   */
  public double averageArea() {
    return averageArea(level);
  }

  /**
   * Return the approximate area of this cell. This method is accurate to within
   * 3% percent for all cell sizes and accurate to within 0.1% for cells at
   * level 5 or higher (i.e. 300km square or smaller). It is moderately cheap to
   * compute.
   */
  public double approxArea() {

    // All cells at the first two levels have the same area.
    if (level < 2) {
      return averageArea(level);
    }

    // First, compute the approximate area of the cell when projected
    // perpendicular to its normal. The cross product of its diagonals gives
    // the normal, and the length of the normal is twice the projected area.
    double flatArea = 0.5 * S2Point.crossProd(
        S2Point.sub(getVertex(2), getVertex(0)), S2Point.sub(getVertex(3), getVertex(1))).norm();

    // Now, compensate for the curvature of the cell surface by pretending
    // that the cell is shaped like a spherical cap. The ratio of the
    // area of a spherical cap to the area of its projected disc turns out
    // to be 2 / (1 + sqrt(1 - r*r)) where "r" is the radius of the disc.
    // For example, when r=0 the ratio is 1, and when r=1 the ratio is 2.
    // Here we set Pi*r*r == flat_area to find the equivalent disc.
    return flatArea * 2 / (1 + Math.sqrt(1 - Math.min(S2.M_1_PI * flatArea, 1.0)));
  }

  /**
   * Return the area of this cell as accurately as possible. This method is more
   * expensive but it is accurate to 6 digits of precision even for leaf cells
   * (whose area is approximately 1e-18).
   */
  public double exactArea() {
    S2Point v0 = getVertex(0);
    S2Point v1 = getVertex(1);
    S2Point v2 = getVertex(2);
    S2Point v3 = getVertex(3);
    return S2.area(v0, v1, v2) + S2.area(v0, v2, v3);
  }

  // //////////////////////////////////////////////////////////////////////
  // S2Region interface (see {@code S2Region} for details):

  @Override
  public S2Region clone() {
    S2Cell clone = new S2Cell();
    clone.face = this.face;
    clone.level = this.level;
    clone.orientation = this.orientation;
    clone.uv = this.uv.clone();

    return clone;
  }

  @Override
  public S2Cap getCapBound() {
    // Use the cell center in (u,v)-space as the cap axis. This vector is
    // very close to GetCenter() and faster to compute. Neither one of these
    // vectors yields the bounding cap with minimal surface area, but they
    // are both pretty close.
    //
    // It's possible to show that the two vertices that are furthest from
    // the (u,v)-origin never determine the maximum cap size (this is a
    // possible future optimization).

    double u = 0.5 * (uv[0][0] + uv[0][1]);
    double v = 0.5 * (uv[1][0] + uv[1][1]);
    S2Cap cap = S2Cap.fromAxisHeight(S2Point.normalize(S2Projections.faceUvToXyz(face, u, v)), 0);
    for (int k = 0; k < 4; ++k) {
      cap = cap.addPoint(getVertex(k));
    }
    return cap;
  }

  // We grow the bounds slightly to make sure that the bounding rectangle
  // also contains the normalized versions of the vertices. Note that the
  // maximum result magnitude is Pi, with a floating-point exponent of 1.
  // Therefore adding or subtracting 2**-51 will always change the result.
  private static final double MAX_ERROR = 1.0 / (1L << 51);

  // The 4 cells around the equator extend to +/-45 degrees latitude at the
  // midpoints of their top and bottom edges. The two cells covering the
  // poles extend down to +/-35.26 degrees at their vertices.
  // adding kMaxError (as opposed to the C version) because of asin and atan2
  // roundoff errors
  private static final double POLE_MIN_LAT = Math.asin(Math.sqrt(1.0 / 3.0)) - MAX_ERROR;
  // 35.26 degrees


  @Override
  public S2LatLngRect getRectBound() {
    if (level > 0) {
      // Except for cells at level 0, the latitude and longitude extremes are
      // attained at the vertices. Furthermore, the latitude range is
      // determined by one pair of diagonally opposite vertices and the
      // longitude range is determined by the other pair.
      //
      // We first determine which corner (i,j) of the cell has the largest
      // absolute latitude. To maximize latitude, we want to find the point in
      // the cell that has the largest absolute z-coordinate and the smallest
      // absolute x- and y-coordinates. To do this we look at each coordinate
      // (u and v), and determine whether we want to minimize or maximize that
      // coordinate based on the axis direction and the cell's (u,v) quadrant.
      double u = uv[0][0] + uv[0][1];
      double v = uv[1][0] + uv[1][1];
      int i = S2Projections.getUAxis(face).z == 0 ? (u < 0 ? 1 : 0) : (u > 0 ? 1 : 0);
      int j = S2Projections.getVAxis(face).z == 0 ? (v < 0 ? 1 : 0) : (v > 0 ? 1 : 0);


      R1Interval lat = R1Interval.fromPointPair(getLatitude(i, j), getLatitude(1 - i, 1 - j));
      lat = lat.expanded(MAX_ERROR).intersection(S2LatLngRect.fullLat());
      if (lat.lo() == -S2.M_PI_2 || lat.hi() == S2.M_PI_2) {
        return new S2LatLngRect(lat, S1Interval.full());
      }
      S1Interval lng = S1Interval.fromPointPair(getLongitude(i, 1 - j), getLongitude(1 - i, j));
      return new S2LatLngRect(lat, lng.expanded(MAX_ERROR));
    }


    // The face centers are the +X, +Y, +Z, -X, -Y, -Z axes in that order.
    // assert (S2Projections.getNorm(face).get(face % 3) == ((face < 3) ? 1 : -1));
    switch (face) {
      case 0:
        return new S2LatLngRect(
            new R1Interval(-S2.M_PI_4, S2.M_PI_4), new S1Interval(-S2.M_PI_4, S2.M_PI_4));
      case 1:
        return new S2LatLngRect(
            new R1Interval(-S2.M_PI_4, S2.M_PI_4), new S1Interval(S2.M_PI_4, 3 * S2.M_PI_4));
      case 2:
        return new S2LatLngRect(
            new R1Interval(POLE_MIN_LAT, S2.M_PI_2), new S1Interval(-S2.M_PI, S2.M_PI));
      case 3:
        return new S2LatLngRect(
            new R1Interval(-S2.M_PI_4, S2.M_PI_4), new S1Interval(3 * S2.M_PI_4, -3 * S2.M_PI_4));
      case 4:
        return new S2LatLngRect(
            new R1Interval(-S2.M_PI_4, S2.M_PI_4), new S1Interval(-3 * S2.M_PI_4, -S2.M_PI_4));
      default:
        return new S2LatLngRect(
            new R1Interval(-S2.M_PI_2, -POLE_MIN_LAT), new S1Interval(-S2.M_PI, S2.M_PI));
    }

  }

  @Override
  public boolean mayIntersect(S2Cell cell) {
    return cellId.intersects(cell.cellId);
  }

  public boolean contains(S2Point p) {
    // We can't just call XYZtoFaceUV, because for points that lie on the
    // boundary between two faces (i.e. u or v is +1/-1) we need to return
    // true for both adjacent cells.
    R2Vector uvPoint = S2Projections.faceXyzToUv(face, p);
    if (uvPoint == null) {
      return false;
    }
    return (uvPoint.x() >= uv[0][0] && uvPoint.x() <= uv[0][1]
        && uvPoint.y() >= uv[1][0] && uvPoint.y() <= uv[1][1]);
  }

  // The point 'p' does not need to be normalized.
  @Override
  public boolean contains(S2Cell cell) {
    return cellId.contains(cell.cellId);
  }

  private void init(S2CellId id) {
    cellId = id;
    MutableInteger ij[] = new MutableInteger[2];
    MutableInteger mOrientation = new MutableInteger(0);

    for (int d = 0; d < 2; ++d) {
      ij[d] = new MutableInteger(0);
    }

    face = (byte) id.toFaceIJOrientation(ij[0], ij[1], mOrientation);
    orientation = (byte) mOrientation.intValue(); // Compress int to a byte.
    level = (byte) id.level();
    int cellSize = 1 << (S2CellId.MAX_LEVEL - level);
    for (int d = 0; d < 2; ++d) {
      // Compute the cell bounds in scaled (i,j) coordinates.
      int sijLo = (ij[d].intValue() & -cellSize) * 2 - MAX_CELL_SIZE;
      int sijHi = sijLo + cellSize * 2;
      uv[d][0] = S2Projections.stToUV((1.0 / MAX_CELL_SIZE) * sijLo);
      uv[d][1] = S2Projections.stToUV((1.0 / MAX_CELL_SIZE) * sijHi);
    }
  }


  // Internal method that does the actual work in the constructors.

  private double getLatitude(int i, int j) {
    S2Point p = S2Projections.faceUvToXyz(face, uv[0][i], uv[1][j]);
    return Math.atan2(p.z, Math.sqrt(p.x * p.x + p.y * p.y));
  }

  private double getLongitude(int i, int j) {
    S2Point p = S2Projections.faceUvToXyz(face, uv[0][i], uv[1][j]);
    return Math.atan2(p.y, p.x);
  }

  // Return the latitude or longitude of the cell vertex given by (i,j),
  // where "i" and "j" are either 0 or 1.

  @Override
  public String toString() {
    return "[" + face + ", " + level + ", " + orientation + ", " + cellId + "]";
  }

  @Override
  public int hashCode() {
    int value = 17;
    value = 37 * (37 * (37 * value + face) + orientation) + level;
    return 37 * value + id().hashCode();
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof S2Cell) {
      S2Cell thatCell = (S2Cell) that;
      return this.face == thatCell.face && this.level == thatCell.level
          && this.orientation == thatCell.orientation && this.cellId.equals(thatCell.cellId);
    }
    return false;
  }

}
