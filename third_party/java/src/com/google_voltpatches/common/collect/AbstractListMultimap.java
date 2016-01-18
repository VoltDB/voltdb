/*
 * Copyright (C) 2007 The Guava Authors
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

import com.google_voltpatches.common.annotations.GwtCompatible;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation_voltpatches.Nullable;

/**
 * Basic implementation of the {@link ListMultimap} interface. It's a wrapper
 * around {@link AbstractMapBasedMultimap} that converts the returned collections into
 * {@code Lists}. The {@link #createCollection} method must return a {@code
 * List}.
 *
 * @author Jared Levy
 * @since 2.0
 */
@GwtCompatible
abstract class AbstractListMultimap<K, V> extends AbstractMapBasedMultimap<K, V>
    implements ListMultimap<K, V> {
  /**
   * Creates a new multimap that uses the provided map.
   *
   * @param map place to store the mapping from each key to its corresponding
   *     values
   */
  protected AbstractListMultimap(Map<K, Collection<V>> map) {
    super(map);
  }

  @Override
  abstract List<V> createCollection();

  @Override
  List<V> createUnmodifiableEmptyCollection() {
    return ImmutableList.of();
  }

  // Following Javadoc copied from ListMultimap.

  /**
   * {@inheritDoc}
   *
   * <p>Because the values for a given key may have duplicates and follow the
   * insertion ordering, this method returns a {@link List}, instead of the
   * {@link Collection} specified in the {@link Multimap} interface.
   */
  @Override
  public List<V> get(@Nullable K key) {
    return (List<V>) super.get(key);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Because the values for a given key may have duplicates and follow the
   * insertion ordering, this method returns a {@link List}, instead of the
   * {@link Collection} specified in the {@link Multimap} interface.
   */
  @Override
  public List<V> removeAll(@Nullable Object key) {
    return (List<V>) super.removeAll(key);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Because the values for a given key may have duplicates and follow the
   * insertion ordering, this method returns a {@link List}, instead of the
   * {@link Collection} specified in the {@link Multimap} interface.
   */
  @Override
  public List<V> replaceValues(@Nullable K key, Iterable<? extends V> values) {
    return (List<V>) super.replaceValues(key, values);
  }

  /**
   * Stores a key-value pair in the multimap.
   *
   * @param key key to store in the multimap
   * @param value value to store in the multimap
   * @return {@code true} always
   */
  @Override
  public boolean put(@Nullable K key, @Nullable V value) {
    return super.put(key, value);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Though the method signature doesn't say so explicitly, the returned map
   * has {@link List} values.
   */
  @Override
  public Map<K, Collection<V>> asMap() {
    return super.asMap();
  }

  /**
   * Compares the specified object to this multimap for equality.
   *
   * <p>Two {@code ListMultimap} instances are equal if, for each key, they
   * contain the same values in the same order. If the value orderings disagree,
   * the multimaps will not be considered equal.
   */
  @Override
  public boolean equals(@Nullable Object object) {
    return super.equals(object);
  }

  private static final long serialVersionUID = 6588350623831699109L;
}
