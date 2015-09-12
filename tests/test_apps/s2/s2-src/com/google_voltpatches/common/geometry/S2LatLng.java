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
 * This class represents a point on the unit sphere as a pair of
 * latitude-longitude coordinates. Like the rest of the "geometry" package, the
 * intent is to represent spherical geometry as a mathematical abstraction, so
 * functions that are specifically related to the Earth's geometry (e.g.
 * easting/northing conversions) should be put elsewhere.
 *
 */
public strictfp class S2LatLng {

  /**
   * Approximate "effective" radius of the Earth in meters.
   */
  public static final double EARTH_RADIUS_METERS = 6367000.0;

  /** The center point the lat/lng coordinate system. */
  public static final S2LatLng CENTER = new S2LatLng(0.0, 0.0);

  private final double latRadians;
  private final double lngRadians;

  public static S2LatLng fromRadians(double latRadians, double lngRadians) {
    return new S2LatLng(latRadians, lngRadians);
  }

  public static S2LatLng fromDegrees(double latDegrees, double lngDegrees) {
    return new S2LatLng(S1Angle.degrees(latDegrees), S1Angle.degrees(lngDegrees));
  }

  public static S2LatLng fromE5(long latE5, long lngE5) {
    return new S2LatLng(S1Angle.e5(latE5), S1Angle.e5(lngE5));
  }

  public static S2LatLng fromE6(long latE6, long lngE6) {
    return new S2LatLng(S1Angle.e6(latE6), S1Angle.e6(lngE6));
  }

  public static S2LatLng fromE7(long latE7, long lngE7) {
    return new S2LatLng(S1Angle.e7(latE7), S1Angle.e7(lngE7));
  }

  public static S1Angle latitude(S2Point p) {
    // We use atan2 rather than asin because the input vector is not necessarily
    // unit length, and atan2 is much more accurate than asin near the poles.
    return S1Angle.radians(
        Math.atan2(p.get(2), Math.sqrt(p.get(0) * p.get(0) + p.get(1) * p.get(1))));
  }

  public static S1Angle longitude(S2Point p) {
    // Note that atan2(0, 0) is defined to be zero.
    return S1Angle.radians(Math.atan2(p.get(1), p.get(0)));
  }

  /** This is internal to avoid ambiguity about which units are expected. */
  private S2LatLng(double latRadians, double lngRadians) {
    this.latRadians = latRadians;
    this.lngRadians = lngRadians;
  }

  /**
   * Basic constructor. The latitude and longitude must be within the ranges
   * allowed by is_valid() below.
   *
   * TODO(dbeaumont): Make this a static factory method (fromLatLng() ?).
   */
  public S2LatLng(S1Angle lat, S1Angle lng) {
    this(lat.radians(), lng.radians());
  }

  /**
   * Default constructor for convenience when declaring arrays, etc.
   *
   * TODO(dbeaumont): Remove the default constructor (just use CENTER).
   */
  public S2LatLng() {
    this(0, 0);
  }

  /**
   * Convert a point (not necessarily normalized) to an S2LatLng.
   *
   * TODO(dbeaumont): Make this a static factory method (fromPoint() ?).
   */
  public S2LatLng(S2Point p) {
    this(Math.atan2(p.z, Math.sqrt(p.x * p.x + p.y * p.y)), Math.atan2(p.y, p.x));
    // The latitude and longitude are already normalized. We use atan2 to
    // compute the latitude because the input vector is not necessarily unit
    // length, and atan2 is much more accurate than asin near the poles.
    // Note that atan2(0, 0) is defined to be zero.
  }

  /** Returns the latitude of this point as a new S1Angle. */
  public S1Angle lat() {
    return S1Angle.radians(latRadians);
  }

  /** Returns the latitude of this point as radians. */
  public double latRadians() {
    return latRadians;
  }

  /** Returns the latitude of this point as degrees. */
  public double latDegrees() {
    return 180.0 / Math.PI * latRadians;
  }

  /** Returns the longitude of this point as a new S1Angle. */
  public S1Angle lng() {
    return S1Angle.radians(lngRadians);
  }

  /** Returns the longitude of this point as radians. */
  public double lngRadians() {
    return lngRadians;
  }

  /** Returns the longitude of this point as degrees. */
  public double lngDegrees() {
    return 180.0 / Math.PI * lngRadians;
  }

  /**
   * Return true if the latitude is between -90 and 90 degrees inclusive and the
   * longitude is between -180 and 180 degrees inclusive.
   */
  public boolean isValid() {
    return Math.abs(lat().radians()) <= S2.M_PI_2 && Math.abs(lng().radians()) <= S2.M_PI;
  }

  /**
   * Returns a new S2LatLng based on this instance for which {@link #isValid()}
   * will be {@code true}.
   * <ul>
   * <li>Latitude is clipped to the range {@code [-90, 90]}
   * <li>Longitude is normalized to be in the range {@code [-180, 180]}
   * </ul>
   * <p>If the current point is valid then the returned point will have the same
   * coordinates.
   */
  public S2LatLng normalized() {
    // drem(x, 2 * S2.M_PI) reduces its argument to the range
    // [-S2.M_PI, S2.M_PI] inclusive, which is what we want here.
    return new S2LatLng(Math.max(-S2.M_PI_2, Math.min(S2.M_PI_2, lat().radians())),
        Math.IEEEremainder(lng().radians(), 2 * S2.M_PI));
  }

  // Clamps the latitude to the range [-90, 90] degrees, and adds or subtracts
  // a multiple of 360 degrees to the longitude if necessary to reduce it to
  // the range [-180, 180].

  /** Convert an S2LatLng to the equivalent unit-length vector (S2Point). */
  public S2Point toPoint() {
    double phi = lat().radians();
    double theta = lng().radians();
    double cosphi = Math.cos(phi);
    return new S2Point(Math.cos(theta) * cosphi, Math.sin(theta) * cosphi, Math.sin(phi));
  }

  /**
   * Return the distance (measured along the surface of the sphere) to the given
   * point.
   */
  public S1Angle getDistance(final S2LatLng o) {
    // This implements the Haversine formula, which is numerically stable for
    // small distances but only gets about 8 digits of precision for very large
    // distances (e.g. antipodal points). Note that 8 digits is still accurate
    // to within about 10cm for a sphere the size of the Earth.
    //
    // This could be fixed with another sin() and cos() below, but at that point
    // you might as well just convert both arguments to S2Points and compute the
    // distance that way (which gives about 15 digits of accuracy for all
    // distances).

    double lat1 = lat().radians();
    double lat2 = o.lat().radians();
    double lng1 = lng().radians();
    double lng2 = o.lng().radians();
    double dlat = Math.sin(0.5 * (lat2 - lat1));
    double dlng = Math.sin(0.5 * (lng2 - lng1));
    double x = dlat * dlat + dlng * dlng * Math.cos(lat1) * Math.cos(lat2);
    return S1Angle.radians(2 * Math.atan2(Math.sqrt(x), Math.sqrt(Math.max(0.0, 1.0 - x))));
    // Return the distance (measured along the surface of the sphere) to the
    // given S2LatLng. This is mathematically equivalent to:
    //
    // S1Angle::FromRadians(ToPoint().Angle(o.ToPoint())
    //
    // but this implementation is slightly more efficient.
  }

  /**
   * Returns the surface distance to the given point assuming a constant radius.
   */
  public double getDistance(final S2LatLng o, double radius) {
    // TODO(dbeaumont): Maybe check that radius >= 0 ?
    return getDistance(o).radians() * radius;
  }

  /**
   * Returns the surface distance to the given point assuming the default Earth
   * radius of {@link #EARTH_RADIUS_METERS}.
   */
  public double getEarthDistance(final S2LatLng o) {
    return getDistance(o, EARTH_RADIUS_METERS);
  }

  /**
   * Adds the given point to this point.
   * Note that there is no guarantee that the new point will be <em>valid</em>.
   */
  public S2LatLng add(final S2LatLng o) {
    return new S2LatLng(latRadians + o.latRadians, lngRadians + o.lngRadians);
  }

  /**
   * Subtracts the given point from this point.
   * Note that there is no guarantee that the new point will be <em>valid</em>.
   */
  public S2LatLng sub(final S2LatLng o) {
    return new S2LatLng(latRadians - o.latRadians, lngRadians - o.lngRadians);
  }

  /**
   * Scales this point by the given scaling factor.
   * Note that there is no guarantee that the new point will be <em>valid</em>.
   */
  public S2LatLng mul(final double m) {
    // TODO(dbeaumont): Maybe check that m >= 0 ?
    return new S2LatLng(latRadians * m, lngRadians * m);
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof S2LatLng) {
      S2LatLng o = (S2LatLng) that;
      return (latRadians == o.latRadians) && (lngRadians == o.lngRadians);
    }
    return false;
  }

  @Override
  public int hashCode() {
    long value = 17;
    value += 37 * value + Double.doubleToLongBits(latRadians);
    value += 37 * value + Double.doubleToLongBits(lngRadians);
    return (int) (value ^ (value >>> 32));
  }

  /**
   * Returns true if both the latitude and longitude of the given point are
   * within {@code maxError} radians of this point.
   */
  public boolean approxEquals(S2LatLng o, double maxError) {
    return (Math.abs(latRadians - o.latRadians) < maxError)
        && (Math.abs(lngRadians - o.lngRadians) < maxError);
  }

  /**
   * Returns true if the given point is within {@code 1e-9} radians of this
   * point. This corresponds to a distance of less than {@code 1cm} at the
   * surface of the Earth.
   */
  public boolean approxEquals(S2LatLng o) {
    return approxEquals(o, 1e-9);
  }

  @Override
  public String toString() {
    return "(" + latRadians + ", " + lngRadians + ")";
  }

  public String toStringDegrees() {
    return "(" + latDegrees() + ", " + lngDegrees() + ")";
  }
}
