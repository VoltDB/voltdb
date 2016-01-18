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

import java.util.concurrent.ConcurrentMap;

/**
 * A concurrent map which forwards all its method calls to another concurrent
 * map. Subclasses should override one or more methods to modify the behavior of
 * the backing map as desired per the <a
 * href="http://en.wikipedia.org/wiki/Decorator_pattern">decorator pattern</a>.
 *
 * @author Charles Fry
 * @since 2.0
 */
@GwtCompatible
public abstract class ForwardingConcurrentMap<K, V> extends ForwardingMap<K, V>
    implements ConcurrentMap<K, V> {

  /** Constructor for use by subclasses. */
  protected ForwardingConcurrentMap() {}

  @Override
  protected abstract ConcurrentMap<K, V> delegate();

  @Override
  public V putIfAbsent(K key, V value) {
    return delegate().putIfAbsent(key, value);
  }

  @Override
  public boolean remove(Object key, Object value) {
    return delegate().remove(key, value);
  }

  @Override
  public V replace(K key, V value) {
    return delegate().replace(key, value);
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    return delegate().replace(key, oldValue, newValue);
  }
}
