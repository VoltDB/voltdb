/*
 * Copyright (C) 2013 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google_voltpatches.common.collect;

import static com.google_voltpatches.common.base.Preconditions.checkNotNull;

import com.google_voltpatches.common.annotations.GwtCompatible;
import com.google_voltpatches.common.base.Objects;
import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.base.Predicates;
import com.google_voltpatches.j2objc.annotations.Weak;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation_voltpatches.Nullable;

/**
 * Implementation for {@link FilteredMultimap#values()}.
 * 
 * @author Louis Wasserman
 */
@GwtCompatible
final class FilteredMultimapValues<K, V> extends AbstractCollection<V> {
  @Weak private final FilteredMultimap<K, V> multimap;

  FilteredMultimapValues(FilteredMultimap<K, V> multimap) {
    this.multimap = checkNotNull(multimap);
  }

  @Override
  public Iterator<V> iterator() {
    return Maps.valueIterator(multimap.entries().iterator());
  }

  @Override
  public boolean contains(@Nullable Object o) {
    return multimap.containsValue(o);
  }

  @Override
  public int size() {
    return multimap.size();
  }

  @Override
  public boolean remove(@Nullable Object o) {
    Predicate<? super Entry<K, V>> entryPredicate = multimap.entryPredicate();
    for (Iterator<Entry<K, V>> unfilteredItr = multimap.unfiltered().entries().iterator();
        unfilteredItr.hasNext();) {
      Map.Entry<K, V> entry = unfilteredItr.next();
      if (entryPredicate.apply(entry) && Objects.equal(entry.getValue(), o)) {
        unfilteredItr.remove();
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return Iterables.removeIf(
        multimap.unfiltered().entries(),
        // explicit <Entry<K, V>> is required to build with JDK6
        Predicates.<Entry<K, V>>and(
            multimap.entryPredicate(), Maps.<V>valuePredicateOnEntries(Predicates.in(c))));
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return Iterables.removeIf(
        multimap.unfiltered().entries(),
        // explicit <Entry<K, V>> is required to build with JDK6
        Predicates.<Entry<K, V>>and(
            multimap.entryPredicate(),
            Maps.<V>valuePredicateOnEntries(Predicates.not(Predicates.in(c)))));
  }

  @Override
  public void clear() {
    multimap.clear();
  }
}
