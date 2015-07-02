/* Copyright (c) 2001-2011, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.lib;

import java.util.NoSuchElementException;

/**
 * Maintains an ordered  integer->integer lookup table, consisting of two
 * columns, one for keys, the other for values.
 *
 * The table is sorted on either the key or value column, depending on the calls to
 * setKeysSearchTarget() or setValuesSearchTarget(). By default, the table is
 * sorted on values.<p>
 *
 * findXXX() methods return the array index into the list
 * pair containing a matching key or value, or  or -1 if not found.<p>
 *
 * Sorting methods originally contributed by Tony Lai (tony_lai@users dot sourceforge.net).
 * Non-recursive implementation of fast quicksort added by Sergio Bossa sbtourist@users dot sourceforge.net)
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.8.0
 */
public class DoubleIntIndex implements IntLookup, LongLookup {

    private int           count = 0;
    private int           capacity;
    private boolean       sorted       = true;
    private boolean       sortOnValues = true;
    private boolean       hasChanged;
    private final boolean fixedSize;
    private int[]         keys;
    private int[]         values;

//
    private int targetSearchValue;

    public DoubleIntIndex(int capacity, boolean fixedSize) {

        this.capacity  = capacity;
        keys           = new int[capacity];
        values         = new int[capacity];
        this.fixedSize = fixedSize;
        hasChanged     = true;
    }

    public synchronized int getKey(int i) {

        if (i < 0 || i >= count) {
            throw new IndexOutOfBoundsException();
        }

        return keys[i];
    }

    public synchronized int getValue(int i) {

        if (i < 0 || i >= count) {
            throw new IndexOutOfBoundsException();
        }

        return values[i];
    }

    /**
     * Modifies an existing pair.
     * @param i the index
     * @param key the key
     */
    public synchronized void setKey(int i, int key) {

        if (i < 0 || i >= count) {
            throw new IndexOutOfBoundsException();
        }

        if (!sortOnValues) {
            sorted = false;
        }

        keys[i] = key;
    }

    /**
     * Modifies an existing pair.
     * @param i the index
     * @param value the value
     */
    public synchronized void setValue(int i, int value) {

        if (i < 0 || i >= count) {
            throw new IndexOutOfBoundsException();
        }

        if (sortOnValues) {
            sorted = false;
        }

        values[i] = value;
    }

    public synchronized int size() {
        return count;
    }

    public synchronized int capacity() {
        return capacity;
    }

    public int[] getKeys() {
        return keys;
    }

    public int[] getValues() {
        return values;
    }

    public long getTotalValues() {

        long total = 0;

        for (int i = 0; i < count; i++) {
            total += values[i];
        }

        return total;
    }

    public void setSize(int newSize) {
        count = newSize;
    }

    public synchronized boolean addUnsorted(long key, long value) {

        if (key > Integer.MAX_VALUE || key < Integer.MIN_VALUE) {
            throw new java.lang.IllegalArgumentException();
        }

        if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
            throw new java.lang.IllegalArgumentException();
        }

        return addUnsorted((int) key, (int) value);
    }

    /**
     * Adds a pair into the table.
     *
     * @param key the key
     * @param value the value
     * @return true or false depending on success
     */
    public synchronized boolean addUnsorted(int key, int value) {

        if (count == capacity) {
            if (fixedSize) {
                return false;
            } else {
                doubleCapacity();
            }
        }

        if (sorted && count != 0) {
            if (sortOnValues) {
                if (value < values[count - 1]) {
                    sorted = false;
                }
            } else {
                if (key < keys[count - 1]) {
                    sorted = false;
                }
            }
        }

        hasChanged    = true;
        keys[count]   = key;
        values[count] = value;

        count++;

        return true;
    }

    /**
     * Adds a key, value pair into the table with the guarantee that the key
     * is equal or larger than the largest existing key. This prevents a sort
     * from taking place on next call to find()
     *
     * @param key the key
     * @param value the value
     * @return true or false depending on success
     */
    public synchronized boolean addSorted(int key, int value) {

        if (count == capacity) {
            if (fixedSize) {
                return false;
            } else {
                doubleCapacity();
            }
        }

        if (count != 0) {
            if (sortOnValues) {
                if (value < values[count - 1]) {
                    return false;
                }
            } else {
                if (key < keys[count - 1]) {
                    return false;
                }
            }
        }

        hasChanged    = true;
        keys[count]   = key;
        values[count] = value;

        count++;

        return true;
    }

    /**
     * Adds a pair, ensuring no duplicate key xor value already exists in the
     * current search target column.
     * @param key the key
     * @param value the value
     * @return true or false depending on success
     */
    public synchronized boolean addUnique(int key, int value) {

        if (count == capacity) {
            if (fixedSize) {
                return false;
            } else {
                doubleCapacity();
            }
        }

        if (!sorted) {
            fastQuickSort();
        }

        targetSearchValue = sortOnValues ? value
                                         : key;

        int i = binaryEmptySlotSearch();

        if (i == -1) {
            return false;
        }

        hasChanged = true;

        if (count != i) {
            moveRows(i, i + 1, count - i);
        }

        keys[i]   = key;
        values[i] = value;

        count++;

        return true;
    }

    public int add(long key, long value) {

        if (key > Integer.MAX_VALUE || key < Integer.MIN_VALUE) {
            throw new java.lang.IllegalArgumentException();
        }

        if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
            throw new java.lang.IllegalArgumentException();
        }

        return add((int) key, (int) value);
    }

    /**
     * Adds a pair, maintaining sort order on
     * current search target column.
     * @param key the key
     * @param value the value
     * @return index of added key or -1 if full
     */
    public synchronized int add(int key, int value) {

        if (count == capacity) {
            if (fixedSize) {
                return -1;
            } else {
                doubleCapacity();
            }
        }

        if (!sorted) {
            fastQuickSort();
        }

        targetSearchValue = sortOnValues ? value
                                         : key;

        int i = binarySlotSearch();

        if (i == -1) {
            return i;
        }

        hasChanged = true;

        if (count != i) {
            moveRows(i, i + 1, count - i);
        }

        keys[i]   = key;
        values[i] = value;

        count++;

        return i;
    }

    public long lookup(long key) throws NoSuchElementException {

        if (key > Integer.MAX_VALUE || key < Integer.MIN_VALUE) {
            throw new NoSuchElementException();
        }

        return lookup((int) key);
    }

    public int lookup(int key) throws NoSuchElementException {

        if (sortOnValues) {
            sorted       = false;
            sortOnValues = false;
        }

        int i = findFirstEqualKeyIndex(key);

        if (i == -1) {
            throw new NoSuchElementException();
        }

        return getValue(i);
    }

    public long lookup(long key, long def) {

        if (key > Integer.MAX_VALUE || key < Integer.MIN_VALUE) {
            return def;
        }

        if (sortOnValues) {
            sorted       = false;
            sortOnValues = false;
        }

        int i = findFirstEqualKeyIndex((int) key);

        if (i == -1) {
            return def;
        }

        return getValue(i);
    }

    public int lookup(int key, int def) {

        if (sortOnValues) {
            sorted       = false;
            sortOnValues = false;
        }

        int i = findFirstEqualKeyIndex(key);

        if (i == -1) {
            return def;
        }

        return getValue(i);
    }

    public void clear() {
        removeAll();
    }

    public int lookupFirstGreaterEqual(int key) throws NoSuchElementException {

        if (sortOnValues) {
            sorted       = false;
            sortOnValues = false;
        }

        int i = findFirstGreaterEqualKeyIndex(key);

        if (i == -1) {
            throw new NoSuchElementException();
        }

        return getValue(i);
    }

    public synchronized void setValuesSearchTarget() {

        if (!sortOnValues) {
            sorted = false;
        }

        sortOnValues = true;
    }

    public synchronized void setKeysSearchTarget() {

        if (sortOnValues) {
            sorted = false;
        }

        sortOnValues = false;
    }

    /**
     * @param value the value
     * @return the index
     */
    public synchronized int findFirstGreaterEqualKeyIndex(int value) {

        int index = findFirstGreaterEqualSlotIndex(value);

        return index == count ? -1
                              : index;
    }

    /**
     * @param value the value
     * @return the index
     */
    public synchronized int findFirstEqualKeyIndex(int value) {

        if (!sorted) {
            fastQuickSort();
        }

        targetSearchValue = value;

        return binaryFirstSearch();
    }

    /**
     * This method is similar to findFirstGreaterEqualKeyIndex(int) but
     * returns the index of the empty row past the end of the array if
     * the search value is larger than all the values / keys in the searched
     * column.
     * @param value the value
     * @return the index
     */
    public synchronized int findFirstGreaterEqualSlotIndex(int value) {

        if (!sorted) {
            fastQuickSort();
        }

        targetSearchValue = value;

        return binarySlotSearch();
    }

    /**
     * Returns the index of the lowest element == the given search target,
     * or -1
     * @return index or -1 if not found
     */
    private int binaryFirstSearch() {

        int low     = 0;
        int high    = count;
        int mid     = 0;
        int compare = 0;
        int found   = count;

        while (low < high) {
            mid     = (low + high) / 2;
            compare = compare(mid);

            if (compare < 0) {
                high = mid;
            } else if (compare > 0) {
                low = mid + 1;
            } else {
                high  = mid;
                found = mid;
            }
        }

        return found == count ? -1
                              : found;
    }

    /**
     * Returns the index of the lowest element >= the given search target,
     * or count
     *     @return the index
     */
    private int binarySlotSearch() {

        int low     = 0;
        int high    = count;
        int mid     = 0;
        int compare = 0;

        while (low < high) {
            mid     = (low + high) / 2;
            compare = compare(mid);

            if (compare <= 0) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }

        return low;
    }

    /**
     * Returns the index of the lowest element > the given search target
     * or count or -1 if target is found
     * @return the index
     */
    private int binaryEmptySlotSearch() {

        int low     = 0;
        int high    = count;
        int mid     = 0;
        int compare = 0;

        while (low < high) {
            mid     = (low + high) / 2;
            compare = compare(mid);

            if (compare < 0) {
                high = mid;
            } else if (compare > 0) {
                low = mid + 1;
            } else {
                return -1;
            }
        }

        return low;
    }

    public synchronized void sort() {
        fastQuickSort();
    }

    /**
     * fast quicksort using a stack on the heap to reduce stack use
     */
    private synchronized void fastQuickSort() {

        DoubleIntIndex indices   = new DoubleIntIndex(32, false);
        int            threshold = 16;

        indices.push(0, count - 1);

        while (indices.size() > 0) {
            int start = indices.peekKey();
            int end   = indices.peekValue();

            indices.pop();

            if (end - start >= threshold) {
                int pivot = partition(start, end, start + ((end - start) / 2));

                indices.push(start, pivot - 1);
                indices.push(pivot + 1, end);
            } else {
                insertionSort(start, end);
            }
        }

        sorted = true;
    }

    private int partition(int start, int end, int pivot) {

        int store = start;

        swap(pivot, end);

        for (int i = start; i <= end - 1; i++) {
            if (lessThan(i, end)) {
                swap(i, store);

                store++;
            }
        }

        swap(store, end);

        return store;
    }

    /**
     * fast quicksort with recursive quicksort implementation
     */
    private synchronized void fastQuickSortRecursive() {

        quickSort(0, count - 1);
        insertionSort(0, count - 1);

        sorted = true;
    }

    private void quickSort(int l, int r) {

        int M = 16;
        int i;
        int j;
        int v;

        if ((r - l) > M) {
            i = (r + l) / 2;

            if (lessThan(i, l)) {
                swap(l, i);    // Tri-Median Methode!
            }

            if (lessThan(r, l)) {
                swap(l, r);
            }

            if (lessThan(r, i)) {
                swap(i, r);
            }

            j = r - 1;

            swap(i, j);

            i = l;
            v = j;

            for (;;) {
                while (lessThan(++i, v)) {}

                while (lessThan(v, --j)) {}

                if (j < i) {
                    break;
                }

                swap(i, j);
            }

            swap(i, r - 1);
            quickSort(l, j);
            quickSort(i + 1, r);
        }
    }

    private void insertionSort(int lo0, int hi0) {

        int i;
        int j;

        for (i = lo0 + 1; i <= hi0; i++) {
            j = i;

            while ((j > lo0) && lessThan(i, j - 1)) {
                j--;
            }

            if (i != j) {
                moveAndInsertRow(i, j);
            }
        }
    }

    protected void moveAndInsertRow(int i, int j) {

        int col1 = keys[i];
        int col2 = values[i];

        moveRows(j, j + 1, i - j);

        keys[j]   = col1;
        values[j] = col2;
    }

    protected void swap(int i1, int i2) {

        int col1 = keys[i1];
        int col2 = values[i1];

        keys[i1]   = keys[i2];
        values[i1] = values[i2];
        keys[i2]   = col1;
        values[i2] = col2;
    }

    /**
     * Check if targeted column value in the row indexed i is less than the
     * search target object.
     * @param i the index
     * @return -1, 0 or +1
     */
    protected int compare(int i) {

        if (sortOnValues) {
            if (targetSearchValue > values[i]) {
                return 1;
            } else if (targetSearchValue < values[i]) {
                return -1;
            }
        } else {
            if (targetSearchValue > keys[i]) {
                return 1;
            } else if (targetSearchValue < keys[i]) {
                return -1;
            }
        }

        return 0;
    }

    /**
     * Check if row indexed i is less than row indexed j
     * @param i the first index
     * @param j the second index
     * @return true or false
     */
    protected boolean lessThan(int i, int j) {

        if (sortOnValues) {
            if (values[i] < values[j]) {
                return true;
            }
        } else {
            if (keys[i] < keys[j]) {
                return true;
            }
        }

        return false;
    }

    protected void moveRows(int fromIndex, int toIndex, int rows) {
        System.arraycopy(keys, fromIndex, keys, toIndex, rows);
        System.arraycopy(values, fromIndex, values, toIndex, rows);
    }

    protected void doubleCapacity() {

        keys     = (int[]) ArrayUtil.resizeArray(keys, capacity * 2);
        values   = (int[]) ArrayUtil.resizeArray(values, capacity * 2);
        capacity *= 2;
    }

    public void removeRange(int start, int limit) {

        moveRows(limit, start, count - limit);

        count -= (limit - start);
    }

    public void removeAll() {

        hasChanged = true;

        ArrayUtil.clearArray(ArrayUtil.CLASS_CODE_INT, keys, 0, count);
        ArrayUtil.clearArray(ArrayUtil.CLASS_CODE_INT, values, 0, count);

        count = 0;
    }

    public void copyTo(DoubleIntIndex other) {

        System.arraycopy(keys, 0, other.keys, 0, count);
        System.arraycopy(values, 0, other.values, 0, count);
        other.setSize(count);
    }

    public final synchronized void remove(int position) {

        hasChanged = true;

        moveRows(position + 1, position, count - position - 1);

        count--;

        keys[count]   = 0;
        values[count] = 0;
    }

    /**
     * peek the key at top of stack
     * @return int key
     */
    private int peekKey() {
        return getKey(count - 1);
    }

    /**
     * peek the value at top of stack
     * @return int value
     */
    private int peekValue() {
        return getValue(count - 1);
    }

    /**
     * pop the pair at top of stack
     * @return boolean if there was an element
     */
    private boolean pop() {

        if (count > 0) {
            count--;

            return true;
        }

        return false;
    }

    /**
     * push key, value pair
     * @return boolean true if susseful
     */
    private boolean push(int key, int value) {
        return addUnsorted(key, value);
    }
}
