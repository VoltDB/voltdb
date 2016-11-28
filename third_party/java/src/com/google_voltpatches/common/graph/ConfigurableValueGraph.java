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
import static com.google_voltpatches.common.graph.GraphConstants.DEFAULT_NODE_COUNT;
import static com.google_voltpatches.common.graph.GraphConstants.NODE_NOT_IN_GRAPH;
import static com.google_voltpatches.common.graph.Graphs.checkNonNegative;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation_voltpatches.Nullable;

/**
 * Configurable implementation of {@link ValueGraph} that supports the options supplied by {@link
 * AbstractGraphBuilder}.
 *
 * <p>This class maintains a map of nodes to {@link GraphConnections}.
 *
 * <p>Collection-returning accessors return unmodifiable views: the view returned will reflect
 * changes to the graph (if the graph is mutable) but may not be modified by the user.
 *
 * <p>The time complexity of all collection-returning accessors is O(1), since views are returned.
 *
 * @author James Sexton
 * @author Joshua O'Madadhain
 * @author Omar Darwish
 * @param <N> Node parameter type
 * @param <V> Value parameter type
 */
class ConfigurableValueGraph<N, V> extends AbstractValueGraph<N, V> {
  private final boolean isDirected;
  private final boolean allowsSelfLoops;
  private final ElementOrder<N> nodeOrder;

  protected final MapIteratorCache<N, GraphConnections<N, V>> nodeConnections;

  protected long edgeCount; // must be updated when edges are added or removed

  /** Constructs a graph with the properties specified in {@code builder}. */
  ConfigurableValueGraph(AbstractGraphBuilder<? super N> builder) {
    this(
        builder,
        builder.nodeOrder.<N, GraphConnections<N, V>>createMap(
            builder.expectedNodeCount.or(DEFAULT_NODE_COUNT)),
        0L);
  }

  /**
   * Constructs a graph with the properties specified in {@code builder}, initialized with the given
   * node map.
   */
  ConfigurableValueGraph(
      AbstractGraphBuilder<? super N> builder,
      Map<N, GraphConnections<N, V>> nodeConnections,
      long edgeCount) {
    this.isDirected = builder.directed;
    this.allowsSelfLoops = builder.allowsSelfLoops;
    this.nodeOrder = builder.nodeOrder.cast();
    // Prefer the heavier "MapRetrievalCache" for nodes if lookup is expensive.
    this.nodeConnections =
        (nodeConnections instanceof TreeMap)
            ? new MapRetrievalCache<N, GraphConnections<N, V>>(nodeConnections)
            : new MapIteratorCache<N, GraphConnections<N, V>>(nodeConnections);
    this.edgeCount = checkNonNegative(edgeCount);
  }

  @Override
  public Set<N> nodes() {
    return nodeConnections.unmodifiableKeySet();
  }

  @Override
  public boolean isDirected() {
    return isDirected;
  }

  @Override
  public boolean allowsSelfLoops() {
    return allowsSelfLoops;
  }

  @Override
  public ElementOrder<N> nodeOrder() {
    return nodeOrder;
  }

  @Override
  public Set<N> adjacentNodes(Object node) {
    return checkedConnections(node).adjacentNodes();
  }

  @Override
  public Set<N> predecessors(Object node) {
    return checkedConnections(node).predecessors();
  }

  @Override
  public Set<N> successors(Object node) {
    return checkedConnections(node).successors();
  }

  @Override
  public V edgeValueOrDefault(Object nodeU, Object nodeV, @Nullable V defaultValue) {
    GraphConnections<N, V> connectionsU = nodeConnections.get(nodeU);
    if (connectionsU == null) {
      return defaultValue;
    }
    V value = connectionsU.value(nodeV);
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

  @Override
  protected long edgeCount() {
    return edgeCount;
  }

  protected final GraphConnections<N, V> checkedConnections(Object node) {
    GraphConnections<N, V> connections = nodeConnections.get(node);
    if (connections == null) {
      checkNotNull(node);
      throw new IllegalArgumentException(String.format(NODE_NOT_IN_GRAPH, node));
    }
    return connections;
  }

  protected final boolean containsNode(@Nullable Object node) {
    return nodeConnections.containsKey(node);
  }
}
