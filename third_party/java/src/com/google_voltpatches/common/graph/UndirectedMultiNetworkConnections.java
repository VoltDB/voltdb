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

import static com.google_voltpatches.common.base.Preconditions.checkState;
import static com.google_voltpatches.common.graph.GraphConstants.INNER_CAPACITY;
import static com.google_voltpatches.common.graph.GraphConstants.INNER_LOAD_FACTOR;

import com.google_voltpatches.common.collect.HashMultiset;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Multiset;
import com.google_voltpatches.errorprone.annotations.concurrent.LazyInit;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation_voltpatches.Nullable;

/**
 * An implementation of {@link NetworkConnections} for undirected networks with parallel edges.
 *
 * @author James Sexton
 * @param <N> Node parameter type
 * @param <E> Edge parameter type
 */
final class UndirectedMultiNetworkConnections<N, E>
    extends AbstractUndirectedNetworkConnections<N, E> {

  private UndirectedMultiNetworkConnections(Map<E, N> incidentEdges) {
    super(incidentEdges);
  }

  static <N, E> UndirectedMultiNetworkConnections<N, E> of() {
    return new UndirectedMultiNetworkConnections<N, E>(
        new HashMap<E, N>(INNER_CAPACITY, INNER_LOAD_FACTOR));
  }

  static <N, E> UndirectedMultiNetworkConnections<N, E> ofImmutable(Map<E, N> incidentEdges) {
    return new UndirectedMultiNetworkConnections<N, E>(ImmutableMap.copyOf(incidentEdges));
  }

  @LazyInit
  private transient Reference<Multiset<N>> adjacentNodesReference;

  @Override
  public Set<N> adjacentNodes() {
    return Collections.unmodifiableSet(adjacentNodesMultiset().elementSet());
  }

  private Multiset<N> adjacentNodesMultiset() {
    Multiset<N> adjacentNodes = getReference(adjacentNodesReference);
    if (adjacentNodes == null) {
      adjacentNodes = HashMultiset.create(incidentEdgeMap.values());
      adjacentNodesReference = new SoftReference<Multiset<N>>(adjacentNodes);
    }
    return adjacentNodes;
  }

  @Override
  public Set<E> edgesConnecting(final Object node) {
    return new MultiEdgesConnecting<E>(incidentEdgeMap, node) {
      @Override
      public int size() {
        return adjacentNodesMultiset().count(node);
      }
    };
  }

  @Override
  public N removeInEdge(Object edge, boolean isSelfLoop) {
    if (!isSelfLoop) {
      return removeOutEdge(edge);
    }
    return null;
  }

  @Override
  public N removeOutEdge(Object edge) {
    N node = super.removeOutEdge(edge);
    Multiset<N> adjacentNodes = getReference(adjacentNodesReference);
    if (adjacentNodes != null) {
      checkState(adjacentNodes.remove(node));
    }
    return node;
  }

  @Override
  public void addInEdge(E edge, N node, boolean isSelfLoop) {
    if (!isSelfLoop) {
      addOutEdge(edge, node);
    }
  }

  @Override
  public void addOutEdge(E edge, N node) {
    super.addOutEdge(edge, node);
    Multiset<N> adjacentNodes = getReference(adjacentNodesReference);
    if (adjacentNodes != null) {
      checkState(adjacentNodes.add(node));
    }
  }

  @Nullable
  private static <T> T getReference(@Nullable Reference<T> reference) {
    return (reference == null) ? null : reference.get();
  }
}
