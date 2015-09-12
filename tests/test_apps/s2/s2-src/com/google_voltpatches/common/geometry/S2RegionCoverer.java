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

import com.google_voltpatches.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;

/**
 * An S2RegionCoverer is a class that allows arbitrary regions to be
 * approximated as unions of cells (S2CellUnion). This is useful for
 * implementing various sorts of search and precomputation operations.
 *
 * Typical usage: {@code S2RegionCoverer coverer; coverer.setMaxCells(5); S2Cap
 * cap = S2Cap.fromAxisAngle(...); S2CellUnion covering;
 * coverer.getCovering(cap, covering); * }
 *
 * This yields a cell union of at most 5 cells that is guaranteed to cover the
 * given cap (a disc-shaped region on the sphere).
 *
 *  The approximation algorithm is not optimal but does a pretty good job in
 * practice. The output does not always use the maximum number of cells allowed,
 * both because this would not always yield a better approximation, and because
 * max_cells() is a limit on how much work is done exploring the possible
 * covering as well as a limit on the final output size.
 *
 *  One can also generate interior coverings, which are sets of cells which are
 * entirely contained within a region. Interior coverings can be empty, even for
 * non-empty regions, if there are no cells that satisfy the provided
 * constraints and are contained by the region. Note that for performance
 * reasons, it is wise to specify a max_level when computing interior coverings
 * - otherwise for regions with small or zero area, the algorithm may spend a
 * lot of time subdividing cells all the way to leaf level to try to find
 * contained cells.
 *
 *  This class is thread-unsafe. Simultaneous calls to any of the getCovering
 * methods will conflict and produce unpredictable results.
 *
 */
public final strictfp class S2RegionCoverer {

  /**
   * By default, the covering uses at most 8 cells at any level. This gives a
   * reasonable tradeoff between the number of cells used and the accuracy of
   * the approximation (see table below).
   */
  public static final int DEFAULT_MAX_CELLS = 8;

  private static final S2Cell[] FACE_CELLS = new S2Cell[6];
  static {
    for (int face = 0; face < 6; ++face) {
      FACE_CELLS[face] = S2Cell.fromFacePosLevel(face, (byte) 0, 0);
    }
  }


  private int minLevel;
  private int maxLevel;
  private int levelMod;
  private int maxCells;

  // True if we're computing an interior covering.
  private boolean interiorCovering;

  // Counter of number of candidates created, for performance evaluation.
  private int candidatesCreatedCounter;

  /**
   * We save a temporary copy of the pointer passed to GetCovering() in order to
   * avoid passing this parameter around internally. It is only used (and only
   * valid) for the duration of a single GetCovering() call.
   */
  S2Region region;

  /**
   * A temporary variable used by GetCovering() that holds the cell ids that
   * have been added to the covering so far.
   */
  ArrayList<S2CellId> result;


  static class Candidate {
    private S2Cell cell;
    private boolean isTerminal; // Cell should not be expanded further.
    private int numChildren; // Number of children that intersect the region.
    private Candidate[] children; // Actual size may be 0, 4, 16, or 64
    // elements.
  }

  static class QueueEntry {
    private int id;
    private Candidate candidate;

    public QueueEntry(int id, Candidate candidate) {
      this.id = id;
      this.candidate = candidate;
    }
  }

  /**
   * We define our own comparison function on QueueEntries in order to make the
   * results deterministic. Using the default less<QueueEntry>, entries of equal
   * priority would be sorted according to the memory address of the candidate.
   */
  static class QueueEntriesComparator implements Comparator<QueueEntry> {
    @Override
    public int compare(S2RegionCoverer.QueueEntry x, S2RegionCoverer.QueueEntry y) {
      return x.id < y.id ? 1 : (x.id > y.id ? -1 : 0);
    }
  }


  /**
   * We keep the candidates in a priority queue. We specify a vector to hold the
   * queue entries since for some reason priority_queue<> uses a deque by
   * default.
   */
  private PriorityQueue<QueueEntry> candidateQueue;

  /**
   * Default constructor, sets all fields to default values.
   */
  public S2RegionCoverer() {
    minLevel = 0;
    maxLevel = S2CellId.MAX_LEVEL;
    levelMod = 1;
    maxCells = DEFAULT_MAX_CELLS;
    this.region = null;
    result = new ArrayList<S2CellId>();
    // TODO(kirilll?): 10 is a completely random number, work out a better
    // estimate
    candidateQueue = new PriorityQueue<QueueEntry>(10, new QueueEntriesComparator());
  }

  // Set the minimum and maximum cell level to be used. The default is to use
  // all cell levels. Requires: max_level() >= min_level().
  //
  // To find the cell level corresponding to a given physical distance, use
  // the S2Cell metrics defined in s2.h. For example, to find the cell
  // level that corresponds to an average edge length of 10km, use:
  //
  // int level = S2::kAvgEdge.GetClosestLevel(
  // geostore::S2Earth::KmToRadians(length_km));
  //
  // Note: min_level() takes priority over max_cells(), i.e. cells below the
  // given level will never be used even if this causes a large number of
  // cells to be returned.

  /**
   * Sets the minimum level to be used.
   */
  public void setMinLevel(int minLevel) {
    // assert (minLevel >= 0 && minLevel <= S2CellId.MAX_LEVEL);
    this.minLevel = Math.max(0, Math.min(S2CellId.MAX_LEVEL, minLevel));
  }

  /**
   * Sets the maximum level to be used.
   */
  public void setMaxLevel(int maxLevel) {
    // assert (maxLevel >= 0 && maxLevel <= S2CellId.MAX_LEVEL);
    this.maxLevel = Math.max(0, Math.min(S2CellId.MAX_LEVEL, maxLevel));
  }

  public int minLevel() {
    return minLevel;
  }

  public int maxLevel() {
    return maxLevel;
  }

  public int maxCells() {
    return maxCells;
  }

  /**
   * If specified, then only cells where (level - min_level) is a multiple of
   * "level_mod" will be used (default 1). This effectively allows the branching
   * factor of the S2CellId hierarchy to be increased. Currently the only
   * parameter values allowed are 1, 2, or 3, corresponding to branching factors
   * of 4, 16, and 64 respectively.
   */
  public void setLevelMod(int levelMod) {
    // assert (levelMod >= 1 && levelMod <= 3);
    this.levelMod = Math.max(1, Math.min(3, levelMod));
  }

  public int levelMod() {
    return levelMod;
  }


  /**
   * Sets the maximum desired number of cells in the approximation (defaults to
   * kDefaultMaxCells). Note the following:
   *
   * <ul>
   * <li>For any setting of max_cells(), up to 6 cells may be returned if that
   * is the minimum number of cells required (e.g. if the region intersects all
   * six face cells). Up to 3 cells may be returned even for very tiny convex
   * regions if they happen to be located at the intersection of three cube
   * faces.
   *
   * <li>For any setting of max_cells(), an arbitrary number of cells may be
   * returned if min_level() is too high for the region being approximated.
   *
   * <li>If max_cells() is less than 4, the area of the covering may be
   * arbitrarily large compared to the area of the original region even if the
   * region is convex (e.g. an S2Cap or S2LatLngRect).
   * </ul>
   *
   * Accuracy is measured by dividing the area of the covering by the area of
   * the original region. The following table shows the median and worst case
   * values for this area ratio on a test case consisting of 100,000 spherical
   * caps of random size (generated using s2regioncoverer_unittest):
   *
   * <pre>
   * max_cells: 3 4 5 6 8 12 20 100 1000
   * median ratio: 5.33 3.32 2.73 2.34 1.98 1.66 1.42 1.11 1.01
   * worst case: 215518 14.41 9.72 5.26 3.91 2.75 1.92 1.20 1.02
   * </pre>
   */
  public void setMaxCells(int maxCells) {
    this.maxCells = maxCells;
  }

  /**
   * Computes a list of cell ids that covers the given region and satisfies the
   * various restrictions specified above.
   *
   * @param region The region to cover
   * @param covering The list filled in by this method
   */
  public void getCovering(S2Region region, ArrayList<S2CellId> covering) {
    // Rather than just returning the raw list of cell ids generated by
    // GetCoveringInternal(), we construct a cell union and then denormalize it.
    // This has the effect of replacing four child cells with their parent
    // whenever this does not violate the covering parameters specified
    // (min_level, level_mod, etc). This strategy significantly reduces the
    // number of cells returned in many cases, and it is cheap compared to
    // computing the covering in the first place.

    S2CellUnion tmp = getCovering(region);
    tmp.denormalize(minLevel(), levelMod(), covering);
  }

  /**
   * Computes a list of cell ids that is contained within the given region and
   * satisfies the various restrictions specified above.
   *
   * @param region The region to fill
   * @param interior The list filled in by this method
   */
  public void getInteriorCovering(S2Region region, ArrayList<S2CellId> interior) {
    S2CellUnion tmp = getInteriorCovering(region);
    tmp.denormalize(minLevel(), levelMod(), interior);
  }

  /**
   * Return a normalized cell union that covers the given region and satisfies
   * the restrictions *EXCEPT* for min_level() and level_mod(). These criteria
   * cannot be satisfied using a cell union because cell unions are
   * automatically normalized by replacing four child cells with their parent
   * whenever possible. (Note that the list of cell ids passed to the cell union
   * constructor does in fact satisfy all the given restrictions.)
   */
  public S2CellUnion getCovering(S2Region region) {
    S2CellUnion covering = new S2CellUnion();
    getCovering(region, covering);
    return covering;
  }

  public void getCovering(S2Region region, S2CellUnion covering) {
    interiorCovering = false;
    getCoveringInternal(region);
    covering.initSwap(result);
  }

  /**
   * Return a normalized cell union that is contained within the given region
   * and satisfies the restrictions *EXCEPT* for min_level() and level_mod().
   */
  public S2CellUnion getInteriorCovering(S2Region region) {
    S2CellUnion covering = new S2CellUnion();
    getInteriorCovering(region, covering);
    return covering;
  }

  public void getInteriorCovering(S2Region region, S2CellUnion covering) {
    interiorCovering = true;
    getCoveringInternal(region);
    covering.initSwap(result);
  }

  /**
   * Given a connected region and a starting point, return a set of cells at the
   * given level that cover the region.
   */
  public static void getSimpleCovering(
      S2Region region, S2Point start, int level, ArrayList<S2CellId> output) {
    floodFill(region, S2CellId.fromPoint(start).parent(level), output);
  }

  /**
   * If the cell intersects the given region, return a new candidate with no
   * children, otherwise return null. Also marks the candidate as "terminal" if
   * it should not be expanded further.
   */
  private Candidate newCandidate(S2Cell cell) {
    if (!region.mayIntersect(cell)) {
      return null;
    }

    boolean isTerminal = false;
    if (cell.level() >= minLevel) {
      if (interiorCovering) {
        if (region.contains(cell)) {
          isTerminal = true;
        } else if (cell.level() + levelMod > maxLevel) {
          return null;
        }
      } else {
        if (cell.level() + levelMod > maxLevel || region.contains(cell)) {
          isTerminal = true;
        }
      }
    }
    Candidate candidate = new Candidate();
    candidate.cell = cell;
    candidate.isTerminal = isTerminal;
    if (!isTerminal) {
      candidate.children = new Candidate[1 << maxChildrenShift()];
    }
    candidatesCreatedCounter++;
    return candidate;
  }

  /** Return the log base 2 of the maximum number of children of a candidate. */
  private int maxChildrenShift() {
    return 2 * levelMod;
  }

  /**
   * Process a candidate by either adding it to the result list or expanding its
   * children and inserting it into the priority queue. Passing an argument of
   * NULL does nothing.
   */
  private void addCandidate(Candidate candidate) {
    if (candidate == null) {
      return;
    }

    if (candidate.isTerminal) {
      result.add(candidate.cell.id());
      return;
    }
    // assert (candidate.numChildren == 0);

    // Expand one level at a time until we hit min_level_ to ensure that
    // we don't skip over it.
    int numLevels = (candidate.cell.level() < minLevel) ? 1 : levelMod;
    int numTerminals = expandChildren(candidate, candidate.cell, numLevels);

    if (candidate.numChildren == 0) {
      // Do nothing
    } else if (!interiorCovering && numTerminals == 1 << maxChildrenShift()
        && candidate.cell.level() >= minLevel) {
      // Optimization: add the parent cell rather than all of its children.
      // We can't do this for interior coverings, since the children just
      // intersect the region, but may not be contained by it - we need to
      // subdivide them further.
      candidate.isTerminal = true;
      addCandidate(candidate);

    } else {
      // We negate the priority so that smaller absolute priorities are returned
      // first. The heuristic is designed to refine the largest cells first,
      // since those are where we have the largest potential gain. Among cells
      // at the same level, we prefer the cells with the smallest number of
      // intersecting children. Finally, we prefer cells that have the smallest
      // number of children that cannot be refined any further.
      int priority = -((((candidate.cell.level() << maxChildrenShift()) + candidate.numChildren)
          << maxChildrenShift()) + numTerminals);
      candidateQueue.add(new QueueEntry(priority, candidate));
      // logger.info("Push: " + candidate.cell.id() + " (" + priority + ") ");
    }
  }

  /**
   * Populate the children of "candidate" by expanding the given number of
   * levels from the given cell. Returns the number of children that were marked
   * "terminal".
   */
  private int expandChildren(Candidate candidate, S2Cell cell, int numLevels) {
    numLevels--;
    S2Cell[] childCells = new S2Cell[4];
    for (int i = 0; i < 4; ++i) {
      childCells[i] = new S2Cell();
    }
    cell.subdivide(childCells);
    int numTerminals = 0;
    for (int i = 0; i < 4; ++i) {
      if (numLevels > 0) {
        if (region.mayIntersect(childCells[i])) {
          numTerminals += expandChildren(candidate, childCells[i], numLevels);
        }
        continue;
      }
      Candidate child = newCandidate(childCells[i]);
      if (child != null) {
        candidate.children[candidate.numChildren++] = child;
        if (child.isTerminal) {
          ++numTerminals;
        }
      }
    }
    return numTerminals;
  }

  /** Computes a set of initial candidates that cover the given region. */
  private void getInitialCandidates() {
    // Optimization: if at least 4 cells are desired (the normal case),
    // start with a 4-cell covering of the region's bounding cap. This
    // lets us skip quite a few levels of refinement when the region to
    // be covered is relatively small.
    if (maxCells >= 4) {
      // Find the maximum level such that the bounding cap contains at most one
      // cell vertex at that level.
      S2Cap cap = region.getCapBound();
      int level = Math.min(S2Projections.MIN_WIDTH.getMaxLevel(2 * cap.angle().radians()),
          Math.min(maxLevel(), S2CellId.MAX_LEVEL - 1));
      if (levelMod() > 1 && level > minLevel()) {
        level -= (level - minLevel()) % levelMod();
      }
      // We don't bother trying to optimize the level == 0 case, since more than
      // four face cells may be required.
      if (level > 0) {
        // Find the leaf cell containing the cap axis, and determine which
        // subcell of the parent cell contains it.
        ArrayList<S2CellId> base = new ArrayList<S2CellId>(4);
        S2CellId id = S2CellId.fromPoint(cap.axis());
        id.getVertexNeighbors(level, base);
        for (int i = 0; i < base.size(); ++i) {
          addCandidate(newCandidate(new S2Cell(base.get(i))));
        }
        return;
      }
    }
    // Default: start with all six cube faces.
    for (int face = 0; face < 6; ++face) {
      addCandidate(newCandidate(FACE_CELLS[face]));
    }
  }

  /** Generates a covering and stores it in result. */
  private void getCoveringInternal(S2Region region) {
    // Strategy: Start with the 6 faces of the cube. Discard any
    // that do not intersect the shape. Then repeatedly choose the
    // largest cell that intersects the shape and subdivide it.
    //
    // result contains the cells that will be part of the output, while the
    // priority queue contains cells that we may still subdivide further. Cells
    // that are entirely contained within the region are immediately added to
    // the output, while cells that do not intersect the region are immediately
    // discarded.
    // Therefore pq_ only contains cells that partially intersect the region.
    // Candidates are prioritized first according to cell size (larger cells
    // first), then by the number of intersecting children they have (fewest
    // children first), and then by the number of fully contained children
    // (fewest children first).

    Preconditions.checkState(candidateQueue.isEmpty() && result.isEmpty());

    this.region = region;
    candidatesCreatedCounter = 0;

    getInitialCandidates();
    while (!candidateQueue.isEmpty() && (!interiorCovering || result.size() < maxCells)) {
      Candidate candidate = candidateQueue.poll().candidate;
      // logger.info("Pop: " + candidate.cell.id());
      if (candidate.cell.level() < minLevel || candidate.numChildren == 1
          || result.size() + (interiorCovering ? 0 : candidateQueue.size()) + candidate.numChildren
              <= maxCells) {
        // Expand this candidate into its children.
        for (int i = 0; i < candidate.numChildren; ++i) {
          addCandidate(candidate.children[i]);
        }
      } else if (interiorCovering) {
        // Do nothing
      } else {
        candidate.isTerminal = true;
        addCandidate(candidate);
      }
    }

    candidateQueue.clear();
    this.region = null;
  }

  /**
   * Given a region and a starting cell, return the set of all the
   * edge-connected cells at the same level that intersect "region". The output
   * cells are returned in arbitrary order.
   */
  private static void floodFill(S2Region region, S2CellId start, ArrayList<S2CellId> output) {
    HashSet<S2CellId> all = new HashSet<S2CellId>();
    ArrayList<S2CellId> frontier = new ArrayList<S2CellId>();
    output.clear();
    all.add(start);
    frontier.add(start);
    while (!frontier.isEmpty()) {
      S2CellId id = frontier.get(frontier.size() - 1);
      frontier.remove(frontier.size() - 1);
      if (!region.mayIntersect(new S2Cell(id))) {
        continue;
      }
      output.add(id);

      S2CellId[] neighbors = new S2CellId[4];
      id.getEdgeNeighbors(neighbors);
      for (int edge = 0; edge < 4; ++edge) {
        S2CellId nbr = neighbors[edge];
        boolean hasNbr = all.contains(nbr);
        if (!all.contains(nbr)) {
          frontier.add(nbr);
          all.add(nbr);
        }
      }
    }
  }
}
