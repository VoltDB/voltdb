/*
 * Copyright (C) 2014 The Guava Authors
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

import com.google_voltpatches.common.annotations.Beta;
import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.base.Functions;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.graph.GraphConstants.Presence;
import com.google_voltpatches.errorprone.annotations.Immutable;

/**
 * A {@link Graph} whose elements and structural relationships will never change. Instances of this
 * class may be obtained with {@link #copyOf(Graph)}.
 *
 * <p>See the Guava User's Guide's <a
 * href="https://github.com/google/guava/wiki/GraphsExplained#immutable-implementations">discussion
 * of the {@code Immutable*} types</a> for more information on the properties and guarantees
 * provided by this class.
 *
 * @author James Sexton
 * @author Joshua O'Madadhain
 * @author Omar Darwish
 * @param <N> Node parameter type
 * @since 20.0
 */
@Beta
public abstract class ImmutableGraph<N> extends ForwardingGraph<N> {

  /** To ensure the immutability contract is maintained, there must be no public constructors. */
  ImmutableGraph() {}

  /** Returns an immutable copy of {@code graph}. */
  public static <N> ImmutableGraph<N> copyOf(Graph<N> graph) {
    return (graph instanceof ImmutableGraph)
        ? (ImmutableGraph<N>) graph
        : new ValueBackedImpl<N, Presence>(
            GraphBuilder.from(graph), getNodeConnections(graph), graph.edges().size());
  }

  /**
   * Simply returns its argument.
   *
   * @deprecated no need to use this
   */
  @Deprecated
  public static <N> ImmutableGraph<N> copyOf(ImmutableGraph<N> graph) {
    return checkNotNull(graph);
  }

  private static <N> ImmutableMap<N, GraphConnections<N, Presence>> getNodeConnections(
      Graph<N> graph) {
    // ImmutableMap.Builder maintains the order of the elements as inserted, so the map will have
    // whatever ordering the graph's nodes do, so ImmutableSortedMap is unnecessary even if the
    // input nodes are sorted.
    ImmutableMap.Builder<N, GraphConnections<N, Presence>> nodeConnections = ImmutableMap.builder();
    for (N node : graph.nodes()) {
      nodeConnections.put(node, connectionsOf(graph, node));
    }
    return nodeConnections.build();
  }

  private static <N> GraphConnections<N, Presence> connectionsOf(Graph<N> graph, N node) {
    Function<Object, Presence> edgeValueFn = Functions.constant(Presence.EDGE_EXISTS);
    return graph.isDirected()
        ? DirectedGraphConnections.ofImmutable(
            graph.predecessors(node), Maps.asMap(graph.successors(node), edgeValueFn))
        : UndirectedGraphConnections.ofImmutable(
            Maps.asMap(graph.adjacentNodes(node), edgeValueFn));
  }

  static class ValueBackedImpl<N, V> extends ImmutableGraph<N> {
    protected final ValueGraph<N, V> backingValueGraph;

    ValueBackedImpl(
        AbstractGraphBuilder<? super N> builder,
        ImmutableMap<N, GraphConnections<N, V>> nodeConnections,
        long edgeCount) {
      this.backingValueGraph =
          new ConfigurableValueGraph<N, V>(builder, nodeConnections, edgeCount);
    }

    @Override
    protected Graph<N> delegate() {
      return backingValueGraph;
    }
  }
}
