/*
 * Copyright (C) 2008 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google_voltpatches.common.collect;

import static com.google_voltpatches.common.collect.CollectPreconditions.checkNonnegative;

import com.google_voltpatches.common.annotations.GwtCompatible;
import com.google_voltpatches.common.base.Supplier;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.annotation_voltpatches.Nullable;

/**
 * Implementation of {@link Table} using hash tables.
 *
 * <p>The views returned by {@link #column}, {@link #columnKeySet()}, and {@link
 * #columnMap()} have iterators that don't support {@code remove()}. Otherwise,
 * all optional operations are supported. Null row keys, columns keys, and
 * values are not supported.
 *
 * <p>Lookups by row key are often faster than lookups by column key, because
 * the data is stored in a {@code Map<R, Map<C, V>>}. A method call like {@code
 * column(columnKey).get(rowKey)} still runs quickly, since the row key is
 * provided. However, {@code column(columnKey).size()} takes longer, since an
 * iteration across all row keys occurs.
 *
 * <p>Note that this implementation is not synchronized. If multiple threads
 * access this table concurrently and one of the threads modifies the table, it
 * must be synchronized externally.
 * 
 * <p>See the Guava User Guide article on <a href=
 * "http://code.google.com/p/guava-libraries/wiki/NewCollectionTypesExplained#Table">
 * {@code Table}</a>.
 *
 * @author Jared Levy
 * @since 7.0
 */
@GwtCompatible(serializable = true)
public class HashBasedTable<R, C, V> extends StandardTable<R, C, V> {
  private static class Factory<C, V>
      implements Supplier<Map<C, V>>, Serializable {
    final int expectedSize;
    Factory(int expectedSize) {
      this.expectedSize = expectedSize;
    }
    @Override
    public Map<C, V> get() {
      return Maps.newHashMapWithExpectedSize(expectedSize);
    }
    private static final long serialVersionUID = 0;
  }

  /**
   * Creates an empty {@code HashBasedTable}.
   */
  public static <R, C, V> HashBasedTable<R, C, V> create() {
    return new HashBasedTable<R, C, V>(
        new HashMap<R, Map<C, V>>(), new Factory<C, V>(0));
  }

  /**
   * Creates an empty {@code HashBasedTable} with the specified map sizes.
   *
   * @param expectedRows the expected number of distinct row keys
   * @param expectedCellsPerRow the expected number of column key / value
   *     mappings in each row
   * @throws IllegalArgumentException if {@code expectedRows} or {@code
   *     expectedCellsPerRow} is negative
   */
  public static <R, C, V> HashBasedTable<R, C, V> create(
      int expectedRows, int expectedCellsPerRow) {
    checkNonnegative(expectedCellsPerRow, "expectedCellsPerRow");
    Map<R, Map<C, V>> backingMap =
        Maps.newHashMapWithExpectedSize(expectedRows);
    return new HashBasedTable<R, C, V>(
        backingMap, new Factory<C, V>(expectedCellsPerRow));
  }

  /**
   * Creates a {@code HashBasedTable} with the same mappings as the specified
   * table.
   *
   * @param table the table to copy
   * @throws NullPointerException if any of the row keys, column keys, or values
   *     in {@code table} is null
   */
  public static <R, C, V> HashBasedTable<R, C, V> create(
      Table<? extends R, ? extends C, ? extends V> table) {
    HashBasedTable<R, C, V> result = create();
    result.putAll(table);
    return result;
  }

  HashBasedTable(Map<R, Map<C, V>> backingMap, Factory<C, V> factory) {
    super(backingMap, factory);
  }

  // Overriding so NullPointerTester test passes.

  @Override public boolean contains(
      @Nullable Object rowKey, @Nullable Object columnKey) {
    return super.contains(rowKey, columnKey);
  }

  @Override public boolean containsColumn(@Nullable Object columnKey) {
    return super.containsColumn(columnKey);
  }

  @Override public boolean containsRow(@Nullable Object rowKey) {
    return super.containsRow(rowKey);
  }

  @Override public boolean containsValue(@Nullable Object value) {
    return super.containsValue(value);
  }

  @Override public V get(@Nullable Object rowKey, @Nullable Object columnKey) {
    return super.get(rowKey, columnKey);
  }

  @Override public boolean equals(@Nullable Object obj) {
    return super.equals(obj);
  }

  @Override public V remove(
      @Nullable Object rowKey, @Nullable Object columnKey) {
    return super.remove(rowKey, columnKey);
  }

  private static final long serialVersionUID = 0;
}
