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
 * An S2Region represents a two-dimensional region over the unit sphere. It is
 * an abstract interface with various concrete subtypes.
 *
 *  The main purpose of this interface is to allow complex regions to be
 * approximated as simpler regions. So rather than having a wide variety of
 * virtual methods that are implemented by all subtypes, the interface is
 * restricted to methods that are useful for computing approximations.
 *
 *
 */
public interface S2Region {

  /** Return a bounding spherical cap. */
  public abstract S2Cap getCapBound();


  /** Return a bounding latitude-longitude rectangle. */
  public abstract S2LatLngRect getRectBound();

  /**
   * If this method returns true, the region completely contains the given cell.
   * Otherwise, either the region does not contain the cell or the containment
   * relationship could not be determined.
   */
  public abstract boolean contains(S2Cell cell);

  /**
   * If this method returns false, the region does not intersect the given cell.
   * Otherwise, either region intersects the cell, or the intersection
   * relationship could not be determined.
   */
  public abstract boolean mayIntersect(S2Cell cell);
}
