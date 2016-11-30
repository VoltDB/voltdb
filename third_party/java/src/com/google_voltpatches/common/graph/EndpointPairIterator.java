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

import com.google_voltpatches.common.collect.AbstractIterator;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Sets;
import java.util.Iterator;
import java.util.Set;

/**
 * A class to facilitate the set returned by {@link Graph#edges()}.
 *
 * @author James Sexton
 */
abstract class EndpointPairIterator<N> extends AbstractIterator<EndpointPair<N>> {
  private final Graph<N> graph;
  private final Iterator<N> nodeIterator;

  protected N node = null; // null is safe as an initial value because graphs don't allow null nodes
  protected Iterator<N> successorIterator = ImmutableSet.<N>of().iterator();

  static <N> EndpointPairIterator<N> of(Graph<N> graph) {
    return graph.isDirected() ? new Directed<N>(graph) : new Undirected<N>(graph);
  }

  private EndpointPairIterator(Graph<N> graph) {
    this.graph = graph;
    this.nodeIterator = graph.nodes().iterator();
  }

  /**
   * Called after {@link #successorIterator} is exhausted. Advances {@link #node} to the next node
   * and updates {@link #successorIterator} to iterate through the successors of {@link #node}.
   */
  protected final boolean advance() {
    checkState(!successorIterator.hasNext());
    if (!nodeIterator.hasNext()) {
      return false;
    }
    node = nodeIterator.next();
    successorIterator = graph.successors(node).iterator();
    return true;
  }

  /**
   * If the graph is directed, each ordered [source, target] pair will be visited once if there is
   * an edge connecting them.
   */
  private static final class Directed<N> extends EndpointPairIterator<N> {
    private Directed(Graph<N> graph) {
      super(graph);
    }

    @Override
    protected EndpointPair<N> computeNext() {
      while (true) {
        if (successorIterator.hasNext()) {
          return EndpointPair.ordered(node, successorIterator.next());
        }
        if (!advance()) {
          return endOfData();
        }
      }
    }
  }

  /**
   * If the graph is undirected, each unordered [node, otherNode] pair (except self-loops) will be
   * visited twice if there is an edge connecting them. To avoid returning duplicate {@link
   * EndpointPair}s, we keep track of the nodes that we have visited. When processing endpoint
   * pairs, we skip if the "other node" is in the visited set, as shown below:
   *
   * <pre>
   * Nodes = {N1, N2, N3, N4}
   *    N2           __
   *   /  \         |  |
   * N1----N3      N4__|
   *
   * Visited Nodes = {}
   * EndpointPair [N1, N2] - return
   * EndpointPair [N1, N3] - return
   * Visited Nodes = {N1}
   * EndpointPair [N2, N1] - skip
   * EndpointPair [N2, N3] - return
   * Visited Nodes = {N1, N2}
   * EndpointPair [N3, N1] - skip
   * EndpointPair [N3, N2] - skip
   * Visited Nodes = {N1, N2, N3}
   * EndpointPair [N4, N4] - return
   * Visited Nodes = {N1, N2, N3, N4}
   * </pre>
   */
  private static final class Undirected<N> extends EndpointPairIterator<N> {
    private Set<N> visitedNodes;

    private Undirected(Graph<N> graph) {
      super(graph);
      this.visitedNodes = Sets.newHashSetWithExpectedSize(graph.nodes().size());
    }

    @Override
    protected EndpointPair<N> computeNext() {
      while (true) {
        while (successorIterator.hasNext()) {
          N otherNode = successorIterator.next();
          if (!visitedNodes.contains(otherNode)) {
            return EndpointPair.unordered(node, otherNode);
          }
        }
        // Add to visited set *after* processing neighbors so we still include self-loops.
        visitedNodes.add(node);
        if (!advance()) {
          visitedNodes = null;
          return endOfData();
        }
      }
    }
  }
}
