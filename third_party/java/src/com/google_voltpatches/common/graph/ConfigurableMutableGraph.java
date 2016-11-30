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

import com.google_voltpatches.common.graph.GraphConstants.Presence;

/**
 * Configurable implementation of {@link MutableGraph} that supports both directed and undirected
 * graphs. Instances of this class should be constructed with {@link GraphBuilder}.
 *
 * <p>Time complexities for mutation methods are all O(1) except for {@code removeNode(N node)},
 * which is in O(d_node) where d_node is the degree of {@code node}.
 *
 * @author James Sexton
 * @param <N> Node parameter type
 */
final class ConfigurableMutableGraph<N> extends ForwardingGraph<N> implements MutableGraph<N> {
  private final MutableValueGraph<N, Presence> backingValueGraph;

  /** Constructs a {@link MutableGraph} with the properties specified in {@code builder}. */
  ConfigurableMutableGraph(AbstractGraphBuilder<? super N> builder) {
    this.backingValueGraph = new ConfigurableMutableValueGraph<N, Presence>(builder);
  }

  @Override
  protected Graph<N> delegate() {
    return backingValueGraph;
  }

  @Override
  public boolean addNode(N node) {
    return backingValueGraph.addNode(node);
  }

  @Override
  public boolean putEdge(N nodeU, N nodeV) {
    return backingValueGraph.putEdgeValue(nodeU, nodeV, Presence.EDGE_EXISTS) == null;
  }

  @Override
  public boolean removeNode(Object node) {
    return backingValueGraph.removeNode(node);
  }

  @Override
  public boolean removeEdge(Object nodeU, Object nodeV) {
    return backingValueGraph.removeEdge(nodeU, nodeV) != null;
  }
}
