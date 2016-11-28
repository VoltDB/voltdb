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

import static com.google_voltpatches.common.collect.CollectPreconditions.checkEntryNotNull;

import com.google_voltpatches.common.annotations.GwtCompatible;
import com.google_voltpatches.errorprone.annotations.concurrent.LazyInit;
import com.google_voltpatches.j2objc.annotations.RetainedWith;
import javax.annotation_voltpatches.Nullable;

/**
 * Implementation of {@link ImmutableMap} with exactly one entry.
 *
 * @author Jesse Wilson
 * @author Kevin Bourrillion
 */
@GwtCompatible(serializable = true, emulated = true)
@SuppressWarnings("serial") // uses writeReplace(), not default serialization
final class SingletonImmutableBiMap<K, V> extends ImmutableBiMap<K, V> {

  final transient K singleKey;
  final transient V singleValue;

  SingletonImmutableBiMap(K singleKey, V singleValue) {
    checkEntryNotNull(singleKey, singleValue);
    this.singleKey = singleKey;
    this.singleValue = singleValue;
  }

  private SingletonImmutableBiMap(K singleKey, V singleValue, ImmutableBiMap<V, K> inverse) {
    this.singleKey = singleKey;
    this.singleValue = singleValue;
    this.inverse = inverse;
  }

  @Override
  public V get(@Nullable Object key) {
    return singleKey.equals(key) ? singleValue : null;
  }

  @Override
  public int size() {
    return 1;
  }

  @Override
  public boolean containsKey(@Nullable Object key) {
    return singleKey.equals(key);
  }

  @Override
  public boolean containsValue(@Nullable Object value) {
    return singleValue.equals(value);
  }

  @Override
  boolean isPartialView() {
    return false;
  }

  @Override
  ImmutableSet<Entry<K, V>> createEntrySet() {
    return ImmutableSet.of(Maps.immutableEntry(singleKey, singleValue));
  }

  @Override
  ImmutableSet<K> createKeySet() {
    return ImmutableSet.of(singleKey);
  }

  @LazyInit
  @RetainedWith
  transient ImmutableBiMap<V, K> inverse;

  @Override
  public ImmutableBiMap<V, K> inverse() {
    // racy single-check idiom
    ImmutableBiMap<V, K> result = inverse;
    if (result == null) {
      return inverse = new SingletonImmutableBiMap<V, K>(singleValue, singleKey, this);
    } else {
      return result;
    }
  }
}
