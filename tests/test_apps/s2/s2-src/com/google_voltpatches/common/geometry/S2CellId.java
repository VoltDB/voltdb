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

import java.util.List;
import java.util.Locale;

/**
 * An S2CellId is a 64-bit unsigned integer that uniquely identifies a cell in
 * the S2 cell decomposition. It has the following format:
 *
 * <pre>
 * id = [face][face_pos]
 * </pre>
 *
 * face: a 3-bit number (range 0..5) encoding the cube face.
 *
 * face_pos: a 61-bit number encoding the position of the center of this cell
 * along the Hilbert curve over this face (see the Wiki pages for details).
 *
 * Sequentially increasing cell ids follow a continuous space-filling curve over
 * the entire sphere. They have the following properties:
 *  - The id of a cell at level k consists of a 3-bit face number followed by k
 * bit pairs that recursively select one of the four children of each cell. The
 * next bit is always 1, and all other bits are 0. Therefore, the level of a
 * cell is determined by the position of its lowest-numbered bit that is turned
 * on (for a cell at level k, this position is 2 * (MAX_LEVEL - k).)
 *  - The id of a parent cell is at the midpoint of the range of ids spanned by
 * its children (or by its descendants at any level).
 *
 * Leaf cells are often used to represent points on the unit sphere, and this
 * class provides methods for converting directly between these two
 * representations. For cells that represent 2D regions rather than discrete
 * point, it is better to use the S2Cell class.
 *
 *
 */
public final strictfp class S2CellId implements Comparable<S2CellId> {

  // Although only 60 bits are needed to represent the index of a leaf
  // cell, we need an extra bit in order to represent the position of
  // the center of the leaf cell along the Hilbert curve.
  public static final int FACE_BITS = 3;
  public static final int NUM_FACES = 6;
  public static final int MAX_LEVEL = 30; // Valid levels: 0..MAX_LEVEL
  public static final int POS_BITS = 2 * MAX_LEVEL + 1;
  public static final int MAX_SIZE = 1 << MAX_LEVEL;

  // Constant related to unsigned long's
  public static final long MAX_UNSIGNED = -1L; // Equivalent to 0xffffffffffffffffL

  // The following lookup tables are used to convert efficiently between an
  // (i,j) cell index and the corresponding position along the Hilbert curve.
  // "lookup_pos" maps 4 bits of "i", 4 bits of "j", and 2 bits representing the
  // orientation of the current cell into 8 bits representing the order in which
  // that subcell is visited by the Hilbert curve, plus 2 bits indicating the
  // new orientation of the Hilbert curve within that subcell. (Cell
  // orientations are represented as combination of kSwapMask and kInvertMask.)
  //
  // "lookup_ij" is an inverted table used for mapping in the opposite
  // direction.
  //
  // We also experimented with looking up 16 bits at a time (14 bits of position
  // plus 2 of orientation) but found that smaller lookup tables gave better
  // performance. (2KB fits easily in the primary cache.)


  // Values for these constants are *declared* in the *.h file. Even though
  // the declaration specifies a value for the constant, that declaration
  // is not a *definition* of storage for the value. Because the values are
  // supplied in the declaration, we don't need the values here. Failing to
  // define storage causes link errors for any code that tries to take the
  // address of one of these values.
  private static final int LOOKUP_BITS = 4;
  private static final int SWAP_MASK = 0x01;
  private static final int INVERT_MASK = 0x02;

  private static final int[] LOOKUP_POS = new int[1 << (2 * LOOKUP_BITS + 2)];
  private static final int[] LOOKUP_IJ = new int[1 << (2 * LOOKUP_BITS + 2)];

  /**
   * This is the offset required to wrap around from the beginning of the
   * Hilbert curve to the end or vice versa; see next_wrap() and prev_wrap().
   */
  private static final long WRAP_OFFSET = (long) (NUM_FACES) << POS_BITS;

  static {
    initLookupCell(0, 0, 0, 0, 0, 0);
    initLookupCell(0, 0, 0, SWAP_MASK, 0, SWAP_MASK);
    initLookupCell(0, 0, 0, INVERT_MASK, 0, INVERT_MASK);
    initLookupCell(0, 0, 0, SWAP_MASK | INVERT_MASK, 0, SWAP_MASK | INVERT_MASK);
  }

  /**
   * The id of the cell.
   */
  private final long id;

  public S2CellId(long id) {
    this.id = id;
  }

  public S2CellId() {
    this.id = 0;
  }

  /** The default constructor returns an invalid cell id. */
  public static S2CellId none() {
    return new S2CellId();
  }

  /**
   * Returns an invalid cell id guaranteed to be larger than any valid cell id.
   * Useful for creating indexes.
   */
  public static S2CellId sentinel() {
    return new S2CellId(MAX_UNSIGNED); // -1
  }

  /**
   * Return a cell given its face (range 0..5), 61-bit Hilbert curve position
   * within that face, and level (range 0..MAX_LEVEL). The given position will
   * be modified to correspond to the Hilbert curve position at the center of
   * the returned cell. This is a static function rather than a constructor in
   * order to give names to the arguments.
   */
  public static S2CellId fromFacePosLevel(int face, long pos, int level) {
    return new S2CellId((((long) face) << POS_BITS) + (pos | 1)).parent(level);
  }

  /**
   * Return the leaf cell containing the given point (a direction vector, not
   * necessarily unit length).
   */
  public static S2CellId fromPoint(S2Point p) {
    int face = S2Projections.xyzToFace(p);
    R2Vector uv = S2Projections.validFaceXyzToUv(face, p);
    int i = stToIJ(S2Projections.uvToST(uv.x()));
    int j = stToIJ(S2Projections.uvToST(uv.y()));
    return fromFaceIJ(face, i, j);
  }


  /** Return the leaf cell containing the given S2LatLng. */
  public static S2CellId fromLatLng(S2LatLng ll) {
    return fromPoint(ll.toPoint());
  }

  public S2Point toPoint() {
    return S2Point.normalize(toPointRaw());
  }

  /**
   * Return the direction vector corresponding to the center of the given cell.
   * The vector returned by ToPointRaw is not necessarily unit length.
   */
  public S2Point toPointRaw() {
    // First we compute the discrete (i,j) coordinates of a leaf cell contained
    // within the given cell. Given that cells are represented by the Hilbert
    // curve position corresponding at their center, it turns out that the cell
    // returned by ToFaceIJOrientation is always one of two leaf cells closest
    // to the center of the cell (unless the given cell is a leaf cell itself,
    // in which case there is only one possibility).
    //
    // Given a cell of size s >= 2 (i.e. not a leaf cell), and letting (imin,
    // jmin) be the coordinates of its lower left-hand corner, the leaf cell
    // returned by ToFaceIJOrientation() is either (imin + s/2, jmin + s/2)
    // (imin + s/2 - 1, jmin + s/2 - 1). We can distinguish these two cases by
    // looking at the low bit of "i" or "j". In the first case the low bit is
    // zero, unless s == 2 (i.e. the level just above leaf cells) in which case
    // the low bit is one.
    //
    // The following calculation converts (i,j) to the (si,ti) coordinates of
    // the cell center. (We need to multiply the coordinates by a factor of 2
    // so that the center of leaf cells can be represented exactly.)

    MutableInteger i = new MutableInteger(0);
    MutableInteger j = new MutableInteger(0);
    int face = toFaceIJOrientation(i, j, null);
    // System.out.println("i= " + i.intValue() + " j = " + j.intValue());
    int delta = isLeaf() ? 1 : (((i.intValue() ^ (((int) id) >>> 2)) & 1) != 0)
      ? 2 : 0;
    int si = (i.intValue() << 1) + delta - MAX_SIZE;
    int ti = (j.intValue() << 1) + delta - MAX_SIZE;
    return faceSiTiToXYZ(face, si, ti);
  }

  /** Return the S2LatLng corresponding to the center of the given cell. */
  public S2LatLng toLatLng() {
    return new S2LatLng(toPointRaw());
  }


  /** The 64-bit unique identifier for this cell. */
  public long id() {
    return id;
  }

  /** Return true if id() represents a valid cell. */
  public boolean isValid() {
    return face() < NUM_FACES && ((lowestOnBit() & (0x1555555555555555L)) != 0);
  }

  /** Which cube face this cell belongs to, in the range 0..5. */
  public int face() {
    return (int) (id >>> POS_BITS);
  }

  /**
   * The position of the cell center along the Hilbert curve over this face, in
   * the range 0..(2**kPosBits-1).
   */
  public long pos() {
    return (id & (-1L >>> FACE_BITS));
  }

  /** Return the subdivision level of the cell (range 0..MAX_LEVEL). */
  public int level() {
    // Fast path for leaf cells.
    if (isLeaf()) {
      return MAX_LEVEL;
    }
    int x = ((int) id);
    int level = -1;
    if (x != 0) {
      level += 16;
    } else {
      x = (int) (id >>> 32);
    }
    // We only need to look at even-numbered bits to determine the
    // level of a valid cell id.
    x &= -x; // Get lowest bit.
    if ((x & 0x00005555) != 0) {
      level += 8;
    }
    if ((x & 0x00550055) != 0) {
      level += 4;
    }
    if ((x & 0x05050505) != 0) {
      level += 2;
    }
    if ((x & 0x11111111) != 0) {
      level += 1;
    }
    // assert (level >= 0 && level <= MAX_LEVEL);
    return level;
  }



  /**
   * Return true if this is a leaf cell (more efficient than checking whether
   * level() == MAX_LEVEL).
   */
  public boolean isLeaf() {
    return ((int) id & 1) != 0;
  }

  /**
   * Return true if this is a top-level face cell (more efficient than checking
   * whether level() == 0).
   */
  public boolean isFace() {
    return (id & (lowestOnBitForLevel(0) - 1)) == 0;
  }

  /**
   * Return the child position (0..3) of this cell's ancestor at the given
   * level, relative to its parent. The argument should be in the range
   * 1..MAX_LEVEL. For example, child_position(1) returns the position of this
   * cell's level-1 ancestor within its top-level face cell.
   */
  public int childPosition(int level) {
    return (int) (id >>> (2 * (MAX_LEVEL - level) + 1)) & 3;
  }

  // Methods that return the range of cell ids that are contained
  // within this cell (including itself). The range is *inclusive*
  // (i.e. test using >= and <=) and the return values of both
  // methods are valid leaf cell ids.
  //
  // These methods should not be used for iteration. If you want to
  // iterate through all the leaf cells, call child_begin(MAX_LEVEL) and
  // child_end(MAX_LEVEL) instead.
  //
  // It would in fact be error-prone to define a range_end() method,
  // because (range_max().id() + 1) is not always a valid cell id, and the
  // iterator would need to be tested using "<" rather that the usual "!=".
  public S2CellId rangeMin() {
    return new S2CellId(id - (lowestOnBit() - 1));
  }

  public S2CellId rangeMax() {
    return new S2CellId(id + (lowestOnBit() - 1));
  }


  /** Return true if the given cell is contained within this one. */
  public boolean contains(S2CellId other) {
    // assert (isValid() && other.isValid());
    return other.greaterOrEquals(rangeMin()) && other.lessOrEquals(rangeMax());
  }

  /** Return true if the given cell intersects this one. */
  public boolean intersects(S2CellId other) {
    // assert (isValid() && other.isValid());
    return other.rangeMin().lessOrEquals(rangeMax())
      && other.rangeMax().greaterOrEquals(rangeMin());
  }

  public S2CellId parent() {
    // assert (isValid() && level() > 0);
    long newLsb = lowestOnBit() << 2;
    return new S2CellId((id & -newLsb) | newLsb);
  }

  /**
   * Return the cell at the previous level or at the given level (which must be
   * less than or equal to the current level).
   */
  public S2CellId parent(int level) {
    // assert (isValid() && level >= 0 && level <= this.level());
    long newLsb = lowestOnBitForLevel(level);
    return new S2CellId((id & -newLsb) | newLsb);
  }

  public S2CellId childBegin() {
    // assert (isValid() && level() < MAX_LEVEL);
    long oldLsb = lowestOnBit();
    return new S2CellId(id - oldLsb + (oldLsb >>> 2));
  }

  public S2CellId childBegin(int level) {
    // assert (isValid() && level >= this.level() && level <= MAX_LEVEL);
    return new S2CellId(id - lowestOnBit() + lowestOnBitForLevel(level));
  }

  public S2CellId childEnd() {
    // assert (isValid() && level() < MAX_LEVEL);
    long oldLsb = lowestOnBit();
    return new S2CellId(id + oldLsb + (oldLsb >>> 2));
  }

  public S2CellId childEnd(int level) {
    // assert (isValid() && level >= this.level() && level <= MAX_LEVEL);
    return new S2CellId(id + lowestOnBit() + lowestOnBitForLevel(level));
  }

  // Iterator-style methods for traversing the immediate children of a cell or
  // all of the children at a given level (greater than or equal to the current
  // level). Note that the end value is exclusive, just like standard STL
  // iterators, and may not even be a valid cell id. You should iterate using
  // code like this:
  //
  // for(S2CellId c = id.childBegin(); !c.equals(id.childEnd()); c = c.next())
  // ...
  //
  // The convention for advancing the iterator is "c = c.next()", so be sure
  // to use 'equals()' in the loop guard, or compare 64-bit cell id's,
  // rather than "c != id.childEnd()".

  /**
   * Return the next cell at the same level along the Hilbert curve. Works
   * correctly when advancing from one face to the next, but does *not* wrap
   * around from the last face to the first or vice versa.
   */
  public S2CellId next() {
    return new S2CellId(id + (lowestOnBit() << 1));
  }

  /**
   * Return the previous cell at the same level along the Hilbert curve. Works
   * correctly when advancing from one face to the next, but does *not* wrap
   * around from the last face to the first or vice versa.
   */
  public S2CellId prev() {
    return new S2CellId(id - (lowestOnBit() << 1));
  }


  /**
   * Like next(), but wraps around from the last face to the first and vice
   * versa. Should *not* be used for iteration in conjunction with
   * child_begin(), child_end(), Begin(), or End().
   */
  public S2CellId nextWrap() {
    S2CellId n = next();
    if (unsignedLongLessThan(n.id, WRAP_OFFSET)) {
      return n;
    }
    return new S2CellId(n.id - WRAP_OFFSET);
  }

  /**
   * Like prev(), but wraps around from the last face to the first and vice
   * versa. Should *not* be used for iteration in conjunction with
   * child_begin(), child_end(), Begin(), or End().
   */
  public S2CellId prevWrap() {
    S2CellId p = prev();
    if (p.id < WRAP_OFFSET) {
      return p;
    }
    return new S2CellId(p.id + WRAP_OFFSET);
  }


  public static S2CellId begin(int level) {
    return fromFacePosLevel(0, 0, 0).childBegin(level);
  }

  public static S2CellId end(int level) {
    return fromFacePosLevel(5, 0, 0).childEnd(level);
  }


  /**
   * Decodes the cell id from a compact text string suitable for display or
   * indexing. Cells at lower levels (i.e. larger cells) are encoded into
   * fewer characters. The maximum token length is 16.
   *
   * @param token the token to decode
   * @return the S2CellId for that token
   * @throws NumberFormatException if the token is not formatted correctly
   */
  public static S2CellId fromToken(String token) {
    if (token == null) {
      throw new NumberFormatException("Null string in S2CellId.fromToken");
    }
    if (token.length() == 0) {
      throw new NumberFormatException("Empty string in S2CellId.fromToken");
    }
    if (token.length() > 16 || "X".equals(token)) {
      return none();
    }

    long value = 0;
    for (int pos = 0; pos < 16; pos++) {
      int digit = 0;
      if (pos < token.length()) {
        digit = Character.digit(token.charAt(pos), 16);
        if (digit == -1) {
          throw new NumberFormatException(token);
        }
        if (overflowInParse(value, digit)) {
          throw new NumberFormatException("Too large for unsigned long: " + token);
        }
      }
      value = (value * 16) + digit;
    }

    return new S2CellId(value);
  }

  /**
   * Encodes the cell id to compact text strings suitable for display or indexing.
   * Cells at lower levels (i.e. larger cells) are encoded into fewer characters.
   * The maximum token length is 16.
   *
   * Simple implementation: convert the id to hex and strip trailing zeros. We
   * could use base-32 or base-64, but assuming the cells used for indexing
   * regions are at least 100 meters across (level 16 or less), the savings
   * would be at most 3 bytes (9 bytes hex vs. 6 bytes base-64).
   *
   * @return the encoded cell id
   */
  public String toToken() {
    if (id == 0) {
      return "X";
    }

    String hex = Long.toHexString(id).toLowerCase(Locale.ENGLISH);
    StringBuilder sb = new StringBuilder(16);
    for (int i = hex.length(); i < 16; i++) {
      sb.append('0');
    }
    sb.append(hex);
    for (int len = 16; len > 0; len--) {
      if (sb.charAt(len - 1) != '0') {
        return sb.substring(0, len);
      }
    }

    throw new RuntimeException("Shouldn't make it here");
  }

  /**
   * Returns true if (current * 10) + digit is a number too large to be
   * represented by an unsigned long.  This is useful for detecting overflow
   * while parsing a string representation of a number.
   */
  private static boolean overflowInParse(long current, int digit) {
    return overflowInParse(current, digit, 10);
  }

  /**
   * Returns true if (current * radix) + digit is a number too large to be
   * represented by an unsigned long.  This is useful for detecting overflow
   * while parsing a string representation of a number.
   * Does not verify whether supplied radix is valid, passing an invalid radix
   * will give undefined results or an ArrayIndexOutOfBoundsException.
   */
  private static boolean overflowInParse(long current, int digit, int radix) {
    if (current >= 0) {
      if (current < maxValueDivs[radix]) {
        return false;
      }
      if (current > maxValueDivs[radix]) {
        return true;
      }
      // current == maxValueDivs[radix]
      return (digit > maxValueMods[radix]);
    }

    // current < 0: high bit is set
    return true;
  }

  // calculated as 0xffffffffffffffff / radix
  private static final long maxValueDivs[] = {0, 0, // 0 and 1 are invalid
      9223372036854775807L, 6148914691236517205L, 4611686018427387903L, // 2-4
      3689348814741910323L, 3074457345618258602L, 2635249153387078802L, // 5-7
      2305843009213693951L, 2049638230412172401L, 1844674407370955161L, // 8-10
      1676976733973595601L, 1537228672809129301L, 1418980313362273201L, // 11-13
      1317624576693539401L, 1229782938247303441L, 1152921504606846975L, // 14-16
      1085102592571150095L, 1024819115206086200L, 970881267037344821L, // 17-19
      922337203685477580L, 878416384462359600L, 838488366986797800L, // 20-22
      802032351030850070L, 768614336404564650L, 737869762948382064L, // 23-25
      709490156681136600L, 683212743470724133L, 658812288346769700L, // 26-28
      636094623231363848L, 614891469123651720L, 595056260442243600L, // 29-31
      576460752303423487L, 558992244657865200L, 542551296285575047L, // 32-34
      527049830677415760L, 512409557603043100L }; // 35-36

  // calculated as 0xffffffffffffffff % radix
  private static final int maxValueMods[] = {0, 0, // 0 and 1 are invalid
      1, 0, 3, 0, 3, 1, 7, 6, 5, 4, 3, 2, 1, 0, 15, 0, 15, 16, 15, 15, // 2-21
      15, 5, 15, 15, 15, 24, 15, 23, 15, 15, 31, 15, 17, 15, 15 }; // 22-36

  /**
   * Return the four cells that are adjacent across the cell's four edges.
   * Neighbors are returned in the order defined by S2Cell::GetEdge. All
   * neighbors are guaranteed to be distinct.
   */
  public void getEdgeNeighbors(S2CellId neighbors[]) {

    MutableInteger i = new MutableInteger(0);
    MutableInteger j = new MutableInteger(0);

    int level = this.level();
    int size = 1 << (MAX_LEVEL - level);
    int face = toFaceIJOrientation(i, j, null);

    // Edges 0, 1, 2, 3 are in the S, E, N, W directions.
    neighbors[0] = fromFaceIJSame(face, i.intValue(), j.intValue() - size,
      j.intValue() - size >= 0).parent(level);
    neighbors[1] = fromFaceIJSame(face, i.intValue() + size, j.intValue(),
      i.intValue() + size < MAX_SIZE).parent(level);
    neighbors[2] = fromFaceIJSame(face, i.intValue(), j.intValue() + size,
      j.intValue() + size < MAX_SIZE).parent(level);
    neighbors[3] = fromFaceIJSame(face, i.intValue() - size, j.intValue(),
      i.intValue() - size >= 0).parent(level);
  }

  /**
   * Return the neighbors of closest vertex to this cell at the given level, by
   * appending them to "output". Normally there are four neighbors, but the
   * closest vertex may only have three neighbors if it is one of the 8 cube
   * vertices.
   *
   * Requires: level < this.evel(), so that we can determine which vertex is
   * closest (in particular, level == MAX_LEVEL is not allowed).
   */
  public void getVertexNeighbors(int level, List<S2CellId> output) {
    // "level" must be strictly less than this cell's level so that we can
    // determine which vertex this cell is closest to.
    // assert (level < this.level());
    MutableInteger i = new MutableInteger(0);
    MutableInteger j = new MutableInteger(0);
    int face = toFaceIJOrientation(i, j, null);

    // Determine the i- and j-offsets to the closest neighboring cell in each
    // direction. This involves looking at the next bit of "i" and "j" to
    // determine which quadrant of this->parent(level) this cell lies in.
    int halfsize = 1 << (MAX_LEVEL - (level + 1));
    int size = halfsize << 1;
    boolean isame, jsame;
    int ioffset, joffset;
    if ((i.intValue() & halfsize) != 0) {
      ioffset = size;
      isame = (i.intValue() + size) < MAX_SIZE;
    } else {
      ioffset = -size;
      isame = (i.intValue() - size) >= 0;
    }
    if ((j.intValue() & halfsize) != 0) {
      joffset = size;
      jsame = (j.intValue() + size) < MAX_SIZE;
    } else {
      joffset = -size;
      jsame = (j.intValue() - size) >= 0;
    }

    output.add(parent(level));
    output
      .add(fromFaceIJSame(face, i.intValue() + ioffset, j.intValue(), isame)
        .parent(level));
    output
      .add(fromFaceIJSame(face, i.intValue(), j.intValue() + joffset, jsame)
        .parent(level));
    // If i- and j- edge neighbors are *both* on a different face, then this
    // vertex only has three neighbors (it is one of the 8 cube vertices).
    if (isame || jsame) {
      output.add(fromFaceIJSame(face, i.intValue() + ioffset,
        j.intValue() + joffset, isame && jsame).parent(level));
    }
  }

  /**
   * Append all neighbors of this cell at the given level to "output". Two cells
   * X and Y are neighbors if their boundaries intersect but their interiors do
   * not. In particular, two cells that intersect at a single point are
   * neighbors.
   *
   * Requires: nbr_level >= this->level(). Note that for cells adjacent to a
   * face vertex, the same neighbor may be appended more than once.
   */
  public void getAllNeighbors(int nbrLevel, List<S2CellId> output) {
    MutableInteger i = new MutableInteger(0);
    MutableInteger j = new MutableInteger(0);

    int face = toFaceIJOrientation(i, j, null);

    // Find the coordinates of the lower left-hand leaf cell. We need to
    // normalize (i,j) to a known position within the cell because nbr_level
    // may be larger than this cell's level.
    int size = 1 << (MAX_LEVEL - level());
    i.setValue(i.intValue() & -size);
    j.setValue(j.intValue() & -size);

    int nbrSize = 1 << (MAX_LEVEL - nbrLevel);
    // assert (nbrSize <= size);

    // We compute the N-S, E-W, and diagonal neighbors in one pass.
    // The loop test is at the end of the loop to avoid 32-bit overflow.
    for (int k = -nbrSize;; k += nbrSize) {
      boolean sameFace;
      if (k < 0) {
        sameFace = (j.intValue() + k >= 0);
      } else if (k >= size) {
        sameFace = (j.intValue() + k < MAX_SIZE);
      } else {
        sameFace = true;
        // North and South neighbors.
        output.add(fromFaceIJSame(face, i.intValue() + k,
          j.intValue() - nbrSize, j.intValue() - size >= 0).parent(nbrLevel));
        output.add(fromFaceIJSame(face, i.intValue() + k, j.intValue() + size,
          j.intValue() + size < MAX_SIZE).parent(nbrLevel));
      }
      // East, West, and Diagonal neighbors.
      output.add(fromFaceIJSame(face, i.intValue() - nbrSize,
        j.intValue() + k, sameFace && i.intValue() - size >= 0).parent(
        nbrLevel));
      output.add(fromFaceIJSame(face, i.intValue() + size, j.intValue() + k,
        sameFace && i.intValue() + size < MAX_SIZE).parent(nbrLevel));
      if (k >= size) {
        break;
      }
    }
  }

  // ///////////////////////////////////////////////////////////////////
  // Low-level methods.

  /**
   * Return a leaf cell given its cube face (range 0..5) and i- and
   * j-coordinates (see s2.h).
   */
  public static S2CellId fromFaceIJ(int face, int i, int j) {
    // Optimization notes:
    // - Non-overlapping bit fields can be combined with either "+" or "|".
    // Generally "+" seems to produce better code, but not always.

    // gcc doesn't have very good code generation for 64-bit operations.
    // We optimize this by computing the result as two 32-bit integers
    // and combining them at the end. Declaring the result as an array
    // rather than local variables helps the compiler to do a better job
    // of register allocation as well. Note that the two 32-bits halves
    // get shifted one bit to the left when they are combined.
    long n[] = {0, face << (POS_BITS - 33)};

    // Alternating faces have opposite Hilbert curve orientations; this
    // is necessary in order for all faces to have a right-handed
    // coordinate system.
    int bits = (face & SWAP_MASK);

    // Each iteration maps 4 bits of "i" and "j" into 8 bits of the Hilbert
    // curve position. The lookup table transforms a 10-bit key of the form
    // "iiiijjjjoo" to a 10-bit value of the form "ppppppppoo", where the
    // letters [ijpo] denote bits of "i", "j", Hilbert curve position, and
    // Hilbert curve orientation respectively.

    for (int k = 7; k >= 0; --k) {
      bits = getBits(n, i, j, k, bits);
    }

    S2CellId s = new S2CellId((((n[1] << 32) + n[0]) << 1) + 1);
    return s;
  }

  private static int getBits(long[] n, int i, int j, int k, int bits) {
    final int mask = (1 << LOOKUP_BITS) - 1;
    bits += (((i >> (k * LOOKUP_BITS)) & mask) << (LOOKUP_BITS + 2));
    bits += (((j >> (k * LOOKUP_BITS)) & mask) << 2);
    bits = LOOKUP_POS[bits];
    n[k >> 2] |= ((((long) bits) >> 2) << ((k & 3) * 2 * LOOKUP_BITS));
    bits &= (SWAP_MASK | INVERT_MASK);
    return bits;
  }


  /**
   * Return the (face, i, j) coordinates for the leaf cell corresponding to this
   * cell id. Since cells are represented by the Hilbert curve position at the
   * center of the cell, the returned (i,j) for non-leaf cells will be a leaf
   * cell adjacent to the cell center. If "orientation" is non-NULL, also return
   * the Hilbert curve orientation for the current cell.
   */
  public int toFaceIJOrientation(MutableInteger pi, MutableInteger pj,
      MutableInteger orientation) {
    // System.out.println("Entering toFaceIjorientation");
    int face = this.face();
    int bits = (face & SWAP_MASK);

    // System.out.println("face = " + face + " bits = " + bits);

    // Each iteration maps 8 bits of the Hilbert curve position into
    // 4 bits of "i" and "j". The lookup table transforms a key of the
    // form "ppppppppoo" to a value of the form "iiiijjjjoo", where the
    // letters [ijpo] represents bits of "i", "j", the Hilbert curve
    // position, and the Hilbert curve orientation respectively.
    //
    // On the first iteration we need to be careful to clear out the bits
    // representing the cube face.
    for (int k = 7; k >= 0; --k) {
      bits = getBits1(pi, pj, k, bits);
      // System.out.println("pi = " + pi + " pj= " + pj + " bits = " + bits);
    }

    if (orientation != null) {
      // The position of a non-leaf cell at level "n" consists of a prefix of
      // 2*n bits that identifies the cell, followed by a suffix of
      // 2*(MAX_LEVEL-n)+1 bits of the form 10*. If n==MAX_LEVEL, the suffix is
      // just "1" and has no effect. Otherwise, it consists of "10", followed
      // by (MAX_LEVEL-n-1) repetitions of "00", followed by "0". The "10" has
      // no effect, while each occurrence of "00" has the effect of reversing
      // the kSwapMask bit.
      // assert (S2.POS_TO_ORIENTATION[2] == 0);
      // assert (S2.POS_TO_ORIENTATION[0] == S2.SWAP_MASK);
      if ((lowestOnBit() & 0x1111111111111110L) != 0) {
        bits ^= S2.SWAP_MASK;
      }
      orientation.setValue(bits);
    }
    return face;
  }

  private int getBits1(MutableInteger i, MutableInteger j, int k, int bits) {
    final int nbits = (k == 7) ? (MAX_LEVEL - 7 * LOOKUP_BITS) : LOOKUP_BITS;

    bits += (((int) (id >>> (k * 2 * LOOKUP_BITS + 1)) &
            ((1 << (2 * nbits)) - 1))) << 2;
    /*
     * System.out.println("id is: " + id_); System.out.println("bits is " +
     * bits); System.out.println("lookup_ij[bits] is " + lookup_ij[bits]);
     */
    bits = LOOKUP_IJ[bits];
    i.setValue(i.intValue()
      + ((bits >> (LOOKUP_BITS + 2)) << (k * LOOKUP_BITS)));
    /*
     * System.out.println("left is " + ((bits >> 2) & ((1 << kLookupBits) -
     * 1))); System.out.println("right is " + (k * kLookupBits));
     * System.out.println("j is: " + j.intValue()); System.out.println("addition
     * is: " + ((((bits >> 2) & ((1 << kLookupBits) - 1))) << (k *
     * kLookupBits)));
     */
    j.setValue(j.intValue()
      + ((((bits >> 2) & ((1 << LOOKUP_BITS) - 1))) << (k * LOOKUP_BITS)));
    bits &= (SWAP_MASK | INVERT_MASK);
    return bits;
  }

  /** Return the lowest-numbered bit that is on for cells at the given level. */
  public long lowestOnBit() {
    return id & -id;
  }

  /**
   * Return the lowest-numbered bit that is on for this cell id, which is equal
   * to (uint64(1) << (2 * (MAX_LEVEL - level))). So for example, a.lsb() <=
   * b.lsb() if and only if a.level() >= b.level(), but the first test is more
   * efficient.
   */
  public static long lowestOnBitForLevel(int level) {
    return 1L << (2 * (MAX_LEVEL - level));
  }


  /**
   * Return the i- or j-index of the leaf cell containing the given s- or
   * t-value.
   */
  private static int stToIJ(double s) {
    // Converting from floating-point to integers via static_cast is very slow
    // on Intel processors because it requires changing the rounding mode.
    // Rounding to the nearest integer using FastIntRound() is much faster.

    final int m = MAX_SIZE / 2; // scaling multiplier
    return (int) Math
      .max(0, Math.min(2 * m - 1, Math.round(m * s + (m - 0.5))));
  }

  /**
   * Convert (face, si, ti) coordinates (see s2.h) to a direction vector (not
   * necessarily unit length).
   */
  private static S2Point faceSiTiToXYZ(int face, int si, int ti) {
    final double kScale = 1.0 / MAX_SIZE;
    double u = S2Projections.stToUV(kScale * si);
    double v = S2Projections.stToUV(kScale * ti);
    return S2Projections.faceUvToXyz(face, u, v);
  }

  /**
   * Given (i, j) coordinates that may be out of bounds, normalize them by
   * returning the corresponding neighbor cell on an adjacent face.
   */
  private static S2CellId fromFaceIJWrap(int face, int i, int j) {
    // Convert i and j to the coordinates of a leaf cell just beyond the
    // boundary of this face. This prevents 32-bit overflow in the case
    // of finding the neighbors of a face cell, and also means that we
    // don't need to worry about the distinction between (s,t) and (u,v).
    i = Math.max(-1, Math.min(MAX_SIZE, i));
    j = Math.max(-1, Math.min(MAX_SIZE, j));

    // Find the (s,t) coordinates corresponding to (i,j). At least one
    // of these coordinates will be just outside the range [0, 1].
    final double kScale = 1.0 / MAX_SIZE;
    double s = kScale * ((i << 1) + 1 - MAX_SIZE);
    double t = kScale * ((j << 1) + 1 - MAX_SIZE);

    // Find the leaf cell coordinates on the adjacent face, and convert
    // them to a cell id at the appropriate level.
    S2Point p = S2Projections.faceUvToXyz(face, s, t);
    face = S2Projections.xyzToFace(p);
    R2Vector st = S2Projections.validFaceXyzToUv(face, p);
    return fromFaceIJ(face, stToIJ(st.x()), stToIJ(st.y()));
  }

  /**
   * Public helper function that calls FromFaceIJ if sameFace is true, or
   * FromFaceIJWrap if sameFace is false.
   */
  public static S2CellId fromFaceIJSame(int face, int i, int j,
      boolean sameFace) {
    if (sameFace) {
      return S2CellId.fromFaceIJ(face, i, j);
    } else {
      return S2CellId.fromFaceIJWrap(face, i, j);
    }
  }

  @Override
  public boolean equals(Object that) {
    if (!(that instanceof S2CellId)) {
      return false;
    }
    S2CellId x = (S2CellId) that;
    return id() == x.id();
  }

  /**
   * Returns true if x1 < x2, when both values are treated as unsigned.
   */
  public static boolean unsignedLongLessThan(long x1, long x2) {
    return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
  }

  /**
   * Returns true if x1 > x2, when both values are treated as unsigned.
   */
  public static boolean unsignedLongGreaterThan(long x1, long x2) {
    return (x1 + Long.MIN_VALUE) > (x2 + Long.MIN_VALUE);
  }

  public boolean lessThan(S2CellId x) {
    return unsignedLongLessThan(id, x.id);
  }

  public boolean greaterThan(S2CellId x) {
    return unsignedLongGreaterThan(id, x.id);
  }

  public boolean lessOrEquals(S2CellId x) {
    return unsignedLongLessThan(id, x.id) || id == x.id;
  }

  public boolean greaterOrEquals(S2CellId x) {
    return unsignedLongGreaterThan(id, x.id) || id == x.id;
  }

  @Override
  public int hashCode() {
    return (int) ((id >>> 32) + id);
  }


  @Override
  public String toString() {
    return "(face=" + face() + ", pos=" + Long.toHexString(pos()) + ", level="
      + level() + ")";
  }

  private static void initLookupCell(int level, int i, int j,
      int origOrientation, int pos, int orientation) {
    if (level == LOOKUP_BITS) {
      int ij = (i << LOOKUP_BITS) + j;
      LOOKUP_POS[(ij << 2) + origOrientation] = (pos << 2) + orientation;
      LOOKUP_IJ[(pos << 2) + origOrientation] = (ij << 2) + orientation;
    } else {
      level++;
      i <<= 1;
      j <<= 1;
      pos <<= 2;
      // Initialize each sub-cell recursively.
      for (int subPos = 0; subPos < 4; subPos++) {
        int ij = S2.posToIJ(orientation, subPos);
        int orientationMask = S2.posToOrientation(subPos);
        initLookupCell(level, i + (ij >>> 1), j + (ij & 1), origOrientation,
            pos + subPos, orientation ^ orientationMask);
      }
    }
  }

  @Override
  public int compareTo(S2CellId that) {
    return unsignedLongLessThan(this.id, that.id) ? -1 :
        unsignedLongGreaterThan(this.id, that.id) ? 1 : 0;
  }
}
