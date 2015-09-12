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
 * Like an Integer, but mutable :)
 *
 *  Sometimes it is just really convenient to be able to pass a MutableInteger
 * as a parameter to a function, or for synchronization purposes (so that you
 * can guard access to an int value without creating a separate Object just to
 * synchronize on).
 *
 * NOT thread-safe
 *
 */
public class MutableInteger {

  private int value;
  private Integer cachedIntegerValue = null;

  public MutableInteger(final int i) {
    value = i;
  }

  public int intValue() {
    return value;
  }

  public Integer integerValue() {
    if (cachedIntegerValue == null) {
      cachedIntegerValue = intValue();
    }
    return cachedIntegerValue;
  }

  @Override
  public boolean equals(final Object o) {
    return o instanceof MutableInteger && ((MutableInteger) o).value == this.value;
  }

  @Override
  public int hashCode() {
    return integerValue().hashCode();
  }

  public void setValue(final int value) {
    this.value = value;
    cachedIntegerValue = null;
  }

  public void increment() {
    add(1);
  }

  public void add(final int amount) {
    setValue(value + amount);
  }

  public void decrement() {
    subtract(1);
  }

  public void subtract(final int amount) {
    add(amount * -1);
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }
}
