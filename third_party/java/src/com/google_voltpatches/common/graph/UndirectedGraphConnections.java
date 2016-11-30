/*
 * Copyright (C) 2016 The Guava Authors
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

package com.google_voltpatches.common.graph;

import static com.google_voltpatches.common.base.Preconditions.checkNotNull;
import static com.google_voltpatches.common.graph.GraphConstants.INNER_CAPACITY;
import static com.google_voltpatches.common.graph.GraphConstants.INNER_LOAD_FACTOR;

import com.google_voltpatches.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of {@link GraphConnections} for undirected graphs.
 *
 * @author James Sexton
 * @param <N> Node parameter type
 * @param <V> Value parameter type
 */
final class UndirectedGraphConnections<N, V> implements GraphConnections<N, V> {
  private final Map<N, V> adjacentNodeValues;

  private UndirectedGraphConnections(Map<N, V> adjacentNodeValues) {
    this.adjacentNodeValues = checkNotNull(adjacentNodeValues);
  }

  static <N, V> UndirectedGraphConnections<N, V> of() {
    return new UndirectedGraphConnections<N, V>(
        new HashMap<N, V>(INNER_CAPACITY, INNER_LOAD_FACTOR));
  }

  static <N, V> UndirectedGraphConnections<N, V> ofImmutable(Map<N, V> adjacentNodeValues) {
    return new UndirectedGraphConnections<N, V>(ImmutableMap.copyOf(adjacentNodeValues));
  }

  @Override
  public Set<N> adjacentNodes() {
    return Collections.unmodifiableSet(adjacentNodeValues.keySet());
  }

  @Override
  public Set<N> predecessors() {
    return adjacentNodes();
  }

  @Override
  public Set<N> successors() {
    return adjacentNodes();
  }

  @Override
  public V value(Object node) {
    return adjacentNodeValues.get(node);
  }

  @Override
  public void removePredecessor(Object node) {
    @SuppressWarnings("unused")
    V unused = removeSuccessor(node);
  }

  @Override
  public V removeSuccessor(Object node) {
    return adjacentNodeValues.remove(node);
  }

  @Override
  public void addPredecessor(N node, V value) {
    @SuppressWarnings("unused")
    V unused = addSuccessor(node, value);
  }

  @Override
  public V addSuccessor(N node, V value) {
    return adjacentNodeValues.put(node, value);
  }
}
