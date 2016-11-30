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

import static com.google_voltpatches.common.base.Preconditions.checkArgument;
import static com.google_voltpatches.common.graph.GraphConstants.NODE_NOT_IN_GRAPH;

import com.google_voltpatches.common.annotations.Beta;
import com.google_voltpatches.common.base.Objects;
import com.google_voltpatches.common.collect.Iterables;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.errorprone.annotations.CanIgnoreReturnValue;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import javax.annotation_voltpatches.Nullable;

/**
 * Static utility methods for {@link Graph} and {@link Network} instances.
 *
 * @author James Sexton
 * @author Joshua O'Madadhain
 * @since 20.0
 */
@Beta
public final class Graphs {

  private Graphs() {}

  // Graph query methods

  /**
   * Returns true if {@code graph} has at least one cycle. A cycle is defined as a non-empty subset
   * of edges in a graph arranged to form a path (a sequence of adjacent outgoing edges) starting
   * and ending with the same node.
   *
   * <p>This method will detect any non-empty cycle, including self-loops (a cycle of length 1).
   */
  public static boolean hasCycle(Graph<?> graph) {
    int numEdges = graph.edges().size();
    if (numEdges == 0) {
      return false; // An edge-free graph is acyclic by definition.
    }
    if (!graph.isDirected() && numEdges >= graph.nodes().size()) {
      return true; // Optimization for the undirected case: at least one cycle must exist.
    }

    Map<Object, NodeVisitState> visitedNodes =
        Maps.newHashMapWithExpectedSize(graph.nodes().size());
    for (Object node : graph.nodes()) {
      if (subgraphHasCycle(graph, visitedNodes, node, null)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if {@code network} has at least one cycle. A cycle is defined as a non-empty
   * subset of edges in a graph arranged to form a path (a sequence of adjacent outgoing edges)
   * starting and ending with the same node.
   *
   * <p>This method will detect any non-empty cycle, including self-loops (a cycle of length 1).
   */
  public static boolean hasCycle(Network<?, ?> network) {
    // In a directed graph, parallel edges cannot introduce a cycle in an acyclic graph.
    // However, in an undirected graph, any parallel edge induces a cycle in the graph.
    if (!network.isDirected()
        && network.allowsParallelEdges()
        && network.edges().size() > network.asGraph().edges().size()) {
      return true;
    }
    return hasCycle(network.asGraph());
  }

  /**
   * Performs a traversal of the nodes reachable from {@code node}. If we ever reach a node we've
   * already visited (following only outgoing edges and without reusing edges), we know there's a
   * cycle in the graph.
   */
  private static boolean subgraphHasCycle(
      Graph<?> graph,
      Map<Object, NodeVisitState> visitedNodes,
      Object node,
      @Nullable Object previousNode) {
    NodeVisitState state = visitedNodes.get(node);
    if (state == NodeVisitState.COMPLETE) {
      return false;
    }
    if (state == NodeVisitState.PENDING) {
      return true;
    }

    visitedNodes.put(node, NodeVisitState.PENDING);
    for (Object nextNode : graph.successors(node)) {
      if (canTraverseWithoutReusingEdge(graph, nextNode, previousNode)
          && subgraphHasCycle(graph, visitedNodes, nextNode, node)) {
        return true;
      }
    }
    visitedNodes.put(node, NodeVisitState.COMPLETE);
    return false;
  }

  /**
   * Determines whether an edge has already been used during traversal. In the directed case a cycle
   * is always detected before reusing an edge, so no special logic is required. In the undirected
   * case, we must take care not to "backtrack" over an edge (i.e. going from A to B and then going
   * from B to A).
   */
  private static boolean canTraverseWithoutReusingEdge(
      Graph<?> graph, Object nextNode, @Nullable Object previousNode) {
    if (graph.isDirected() || !Objects.equal(previousNode, nextNode)) {
      return true;
    }
    // This falls into the undirected A->B->A case. The Graph interface does not support parallel
    // edges, so this traversal would require reusing the undirected AB edge.
    return false;
  }

  /**
   * Returns the transitive closure of {@code graph}. The transitive closure of a graph is another
   * graph with an edge connecting node A to node B if node B is {@link #reachableNodes(Graph,
   * Object) reachable} from node A.
   *
   * <p>This is a "snapshot" based on the current topology of {@code graph}, rather than a live view
   * of the transitive closure of {@code graph}. In other words, the returned {@link Graph} will not
   * be updated after modifications to {@code graph}.
   */
  // TODO(b/31438252): Consider potential optimizations for this algorithm.
  public static <N> Graph<N> transitiveClosure(Graph<N> graph) {
    MutableGraph<N> transitiveClosure = GraphBuilder.from(graph).allowsSelfLoops(true).build();
    // Every node is, at a minimum, reachable from itself. Since the resulting transitive closure
    // will have no isolated nodes, we can skip adding nodes explicitly and let putEdge() do it.

    if (graph.isDirected()) {
      // Note: works for both directed and undirected graphs, but we only use in the directed case.
      for (N node : graph.nodes()) {
        for (N reachableNode : reachableNodes(graph, node)) {
          transitiveClosure.putEdge(node, reachableNode);
        }
      }
    } else {
      // An optimization for the undirected case: for every node B reachable from node A,
      // node A and node B have the same reachability set.
      Set<N> visitedNodes = new HashSet<N>();
      for (N node : graph.nodes()) {
        if (!visitedNodes.contains(node)) {
          Set<N> reachableNodes = reachableNodes(graph, node);
          visitedNodes.addAll(reachableNodes);
          int pairwiseMatch = 1; // start at 1 to include self-loops
          for (N nodeU : reachableNodes) {
            for (N nodeV : Iterables.limit(reachableNodes, pairwiseMatch++)) {
              transitiveClosure.putEdge(nodeU, nodeV);
            }
          }
        }
      }
    }

    return transitiveClosure;
  }

  /**
   * Returns the set of nodes that are reachable from {@code node}. Node B is defined as reachable
   * from node A if there exists a path (a sequence of adjacent outgoing edges) starting at node A
   * and ending at node B. Note that a node is always reachable from itself via a zero-length path.
   *
   * <p>This is a "snapshot" based on the current topology of {@code graph}, rather than a live view
   * of the set of nodes reachable from {@code node}. In other words, the returned {@link Set} will
   * not be updated after modifications to {@code graph}.
   *
   * @throws IllegalArgumentException if {@code node} is not present in {@code graph}
   */
  @SuppressWarnings("unchecked") // Safe because we only cast if node is an element of the graph.
  public static <N> Set<N> reachableNodes(Graph<N> graph, Object node) {
    checkArgument(graph.nodes().contains(node), NODE_NOT_IN_GRAPH, node);
    Set<N> visitedNodes = new LinkedHashSet<N>();
    Queue<N> queuedNodes = new ArrayDeque<N>();
    visitedNodes.add((N) node);
    queuedNodes.add((N) node);
    // Perform a breadth-first traversal rooted at the input node.
    while (!queuedNodes.isEmpty()) {
      N currentNode = queuedNodes.remove();
      for (N successor : graph.successors(currentNode)) {
        if (visitedNodes.add(successor)) {
          queuedNodes.add(successor);
        }
      }
    }
    return Collections.unmodifiableSet(visitedNodes);
  }

  /**
   * Returns {@code true} if {@code graphA} and {@code graphB} have the same elements and the same
   * relationships between elements, as exposed via the {@link Graph} interface.
   *
   * <p>Thus, two graphs A and B are equivalent if both are null or <b>all</b> of the following are
   * true:
   *
   * <ul>
   * <li>A and B have equal {@link Graph#isDirected() directedness}.
   * <li>A and B have equal {@link Graph#nodes() node sets}.
   * <li>A and B have equal {@link Graph#edges() edge sets}.
   * </ul>
   *
   * <p>Graph properties besides {@link Graph#isDirected() directedness} do <b>not</b> affect
   * equivalence. For example, two graphs may be considered equivalent even if one allows self-loops
   * and the other doesn't. Additionally, the order in which nodes or edges are added to the graph,
   * and the order in which they are iterated over, are irrelevant.
   */
  public static boolean equivalent(@Nullable Graph<?> graphA, @Nullable Graph<?> graphB) {
    if (graphA == graphB) {
      return true;
    }
    if (graphA == null || graphB == null) {
      return false;
    }

    return graphA.isDirected() == graphB.isDirected()
        && graphA.nodes().equals(graphB.nodes())
        && graphA.edges().equals(graphB.edges());
  }

  /**
   * Returns {@code true} if {@code graphA} and {@code graphB} have the same elements (including
   * edge values) and the same relationships between elements, as exposed via the {@link ValueGraph}
   * interface.
   *
   * <p>Thus, two value graphs A and B are equivalent if both are null or <b>all</b> of the
   * following are true:
   *
   * <ul>
   * <li>A and B have equal {@link Graph#isDirected() directedness}.
   * <li>A and B have equal {@link Graph#nodes() node sets}.
   * <li>A and B have equal {@link Graph#edges() edge sets}.
   * <li>Each edge in A has a {@link ValueGraph#edgeValue(Object, Object) value} equal to the {@link
   *     ValueGraph#edgeValue(Object, Object) value} of the corresponding edge in B.
   * </ul>
   *
   * <p>Graph properties besides {@link Graph#isDirected() directedness} do <b>not</b> affect
   * equivalence. For example, two graphs may be considered equivalent even if one allows self-loops
   * and the other doesn't. Additionally, the order in which nodes or edges are added to the graph,
   * and the order in which they are iterated over, are irrelevant.
   */
  public static boolean equivalent(
      @Nullable ValueGraph<?, ?> graphA, @Nullable ValueGraph<?, ?> graphB) {
    if (graphA == graphB) {
      return true;
    }
    if (graphA == null || graphB == null) {
      return false;
    }

    if (graphA.isDirected() != graphB.isDirected()
        || !graphA.nodes().equals(graphB.nodes())
        || !graphA.edges().equals(graphB.edges())) {
      return false;
    }

    for (EndpointPair<?> edge : graphA.edges()) {
      if (!graphA
          .edgeValue(edge.nodeU(), edge.nodeV())
          .equals(graphB.edgeValue(edge.nodeU(), edge.nodeV()))) {
        return false;
      }
    }

    return true;
  }

  /**
   * Returns {@code true} if {@code networkA} and {@code networkB} have the same elements and the
   * same relationships between elements, as exposed via the {@link Network} interface.
   *
   * <p>Thus, two networks A and B are equivalent if both are null or <b>all</b> of the following
   * are true:
   *
   * <ul>
   * <li>A and B have equal {@link Network#isDirected() directedness}.
   * <li>A and B have equal {@link Network#nodes() node sets}.
   * <li>A and B have equal {@link Network#edges() edge sets}.
   * <li>Each edge in A connects the same nodes in the same direction (if any) as the corresponding
   *     edge in B.
   * </ul>
   *
   * <p>Network properties besides {@link Network#isDirected() directedness} do <b>not</b> affect
   * equivalence. For example, two networks may be considered equal even if one allows parallel
   * edges and the other doesn't. Additionally, the order in which nodes or edges are added to the
   * network, and the order in which they are iterated over, are irrelevant.
   */
  public static boolean equivalent(
      @Nullable Network<?, ?> networkA, @Nullable Network<?, ?> networkB) {
    if (networkA == networkB) {
      return true;
    }
    if (networkA == null || networkB == null) {
      return false;
    }

    if (networkA.isDirected() != networkB.isDirected()
        || !networkA.nodes().equals(networkB.nodes())
        || !networkA.edges().equals(networkB.edges())) {
      return false;
    }

    for (Object edge : networkA.edges()) {
      if (!networkA.incidentNodes(edge).equals(networkB.incidentNodes(edge))) {
        return false;
      }
    }

    return true;
  }

  // Graph mutation methods

  // Graph view methods

  /**
   * Returns a view of {@code graph} with the direction (if any) of every edge reversed. All other
   * properties remain intact, and further updates to {@code graph} will be reflected in the view.
   */
  public static <N> Graph<N> transpose(Graph<N> graph) {
    if (!graph.isDirected()) {
      return graph; // the transpose of an undirected graph is an identical graph
    }

    if (graph instanceof TransposedGraph) {
      return ((TransposedGraph<N>) graph).graph;
    }

    return new TransposedGraph<N>(graph);
  }

  private static class TransposedGraph<N> extends AbstractGraph<N> {
    private final Graph<N> graph;

    TransposedGraph(Graph<N> graph) {
      this.graph = graph;
    }

    @Override
    public Set<N> nodes() {
      return graph.nodes();
    }

    /**
     * Defer to {@link AbstractGraph#edges()} (based on {@link #successors(Object)}) for full
     * edges() implementation.
     */
    @Override
    protected long edgeCount() {
      return graph.edges().size();
    }

    @Override
    public boolean isDirected() {
      return graph.isDirected();
    }

    @Override
    public boolean allowsSelfLoops() {
      return graph.allowsSelfLoops();
    }

    @Override
    public ElementOrder<N> nodeOrder() {
      return graph.nodeOrder();
    }

    @Override
    public Set<N> adjacentNodes(Object node) {
      return graph.adjacentNodes(node);
    }

    @Override
    public Set<N> predecessors(Object node) {
      return graph.successors(node); // transpose
    }

    @Override
    public Set<N> successors(Object node) {
      return graph.predecessors(node); // transpose
    }
  }

  /**
   * Returns a view of {@code graph} with the direction (if any) of every edge reversed. All other
   * properties remain intact, and further updates to {@code graph} will be reflected in the view.
   */
  public static <N, V> ValueGraph<N, V> transpose(ValueGraph<N, V> graph) {
    if (!graph.isDirected()) {
      return graph; // the transpose of an undirected graph is an identical graph
    }

    if (graph instanceof TransposedValueGraph) {
      return ((TransposedValueGraph<N, V>) graph).graph;
    }

    return new TransposedValueGraph<N, V>(graph);
  }

  private static class TransposedValueGraph<N, V> extends AbstractValueGraph<N, V> {
    private final ValueGraph<N, V> graph;

    TransposedValueGraph(ValueGraph<N, V> graph) {
      this.graph = graph;
    }

    @Override
    public Set<N> nodes() {
      return graph.nodes();
    }

    /**
     * Defer to {@link AbstractGraph#edges()} (based on {@link #successors(Object)}) for full
     * edges() implementation.
     */
    @Override
    protected long edgeCount() {
      return graph.edges().size();
    }

    @Override
    public boolean isDirected() {
      return graph.isDirected();
    }

    @Override
    public boolean allowsSelfLoops() {
      return graph.allowsSelfLoops();
    }

    @Override
    public ElementOrder<N> nodeOrder() {
      return graph.nodeOrder();
    }

    @Override
    public Set<N> adjacentNodes(Object node) {
      return graph.adjacentNodes(node);
    }

    @Override
    public Set<N> predecessors(Object node) {
      return graph.successors(node); // transpose
    }

    @Override
    public Set<N> successors(Object node) {
      return graph.predecessors(node); // transpose
    }

    @Override
    public V edgeValue(Object nodeU, Object nodeV) {
      return graph.edgeValue(nodeV, nodeU); // transpose
    }

    @Override
    public V edgeValueOrDefault(Object nodeU, Object nodeV, @Nullable V defaultValue) {
      return graph.edgeValueOrDefault(nodeV, nodeU, defaultValue); // transpose
    }
  }

  /**
   * Returns a view of {@code network} with the direction (if any) of every edge reversed. All other
   * properties remain intact, and further updates to {@code network} will be reflected in the view.
   */
  public static <N, E> Network<N, E> transpose(Network<N, E> network) {
    if (!network.isDirected()) {
      return network; // the transpose of an undirected network is an identical network
    }

    if (network instanceof TransposedNetwork) {
      return ((TransposedNetwork<N, E>) network).network;
    }

    return new TransposedNetwork<N, E>(network);
  }

  private static class TransposedNetwork<N, E> extends AbstractNetwork<N, E> {
    private final Network<N, E> network;

    TransposedNetwork(Network<N, E> network) {
      this.network = network;
    }

    @Override
    public Set<N> nodes() {
      return network.nodes();
    }

    @Override
    public Set<E> edges() {
      return network.edges();
    }

    @Override
    public boolean isDirected() {
      return network.isDirected();
    }

    @Override
    public boolean allowsParallelEdges() {
      return network.allowsParallelEdges();
    }

    @Override
    public boolean allowsSelfLoops() {
      return network.allowsSelfLoops();
    }

    @Override
    public ElementOrder<N> nodeOrder() {
      return network.nodeOrder();
    }

    @Override
    public ElementOrder<E> edgeOrder() {
      return network.edgeOrder();
    }

    @Override
    public Set<N> adjacentNodes(Object node) {
      return network.adjacentNodes(node);
    }

    @Override
    public Set<N> predecessors(Object node) {
      return network.successors(node); // transpose
    }

    @Override
    public Set<N> successors(Object node) {
      return network.predecessors(node); // transpose
    }

    @Override
    public Set<E> incidentEdges(Object node) {
      return network.incidentEdges(node);
    }

    @Override
    public Set<E> inEdges(Object node) {
      return network.outEdges(node); // transpose
    }

    @Override
    public Set<E> outEdges(Object node) {
      return network.inEdges(node); // transpose
    }

    @Override
    public EndpointPair<N> incidentNodes(Object edge) {
      EndpointPair<N> endpointPair = network.incidentNodes(edge);
      return EndpointPair.of(network, endpointPair.nodeV(), endpointPair.nodeU()); // transpose
    }

    @Override
    public Set<E> adjacentEdges(Object edge) {
      return network.adjacentEdges(edge);
    }

    @Override
    public Set<E> edgesConnecting(Object nodeU, Object nodeV) {
      return network.edgesConnecting(nodeV, nodeU); // transpose
    }
  }

  // Graph copy methods

  /**
   * Returns the subgraph of {@code graph} induced by {@code nodes}. This subgraph is a new graph
   * that contains all of the nodes in {@code nodes}, and all of the {@link Graph#edges() edges}
   * from {@code graph} for which both nodes are contained by {@code nodes}.
   *
   * @throws IllegalArgumentException if any element in {@code nodes} is not a node in the graph
   */
  public static <N> MutableGraph<N> inducedSubgraph(Graph<N> graph, Iterable<? extends N> nodes) {
    MutableGraph<N> subgraph = GraphBuilder.from(graph).build();
    for (N node : nodes) {
      subgraph.addNode(node);
    }
    for (N node : subgraph.nodes()) {
      for (N successorNode : graph.successors(node)) {
        if (subgraph.nodes().contains(successorNode)) {
          subgraph.putEdge(node, successorNode);
        }
      }
    }
    return subgraph;
  }

  /**
   * Returns the subgraph of {@code graph} induced by {@code nodes}. This subgraph is a new graph
   * that contains all of the nodes in {@code nodes}, and all of the {@link Graph#edges() edges}
   * (and associated edge values) from {@code graph} for which both nodes are contained by {@code
   * nodes}.
   *
   * @throws IllegalArgumentException if any element in {@code nodes} is not a node in the graph
   */
  public static <N, V> MutableValueGraph<N, V> inducedSubgraph(
      ValueGraph<N, V> graph, Iterable<? extends N> nodes) {
    MutableValueGraph<N, V> subgraph = ValueGraphBuilder.from(graph).build();
    for (N node : nodes) {
      subgraph.addNode(node);
    }
    for (N node : subgraph.nodes()) {
      for (N successorNode : graph.successors(node)) {
        if (subgraph.nodes().contains(successorNode)) {
          subgraph.putEdgeValue(node, successorNode, graph.edgeValue(node, successorNode));
        }
      }
    }
    return subgraph;
  }

  /**
   * Returns the subgraph of {@code network} induced by {@code nodes}. This subgraph is a new graph
   * that contains all of the nodes in {@code nodes}, and all of the {@link Network#edges() edges}
   * from {@code network} for which the {@link Network#incidentNodes(Object) incident nodes} are
   * both contained by {@code nodes}.
   *
   * @throws IllegalArgumentException if any element in {@code nodes} is not a node in the graph
   */
  public static <N, E> MutableNetwork<N, E> inducedSubgraph(
      Network<N, E> network, Iterable<? extends N> nodes) {
    MutableNetwork<N, E> subgraph = NetworkBuilder.from(network).build();
    for (N node : nodes) {
      subgraph.addNode(node);
    }
    for (N node : subgraph.nodes()) {
      for (E edge : network.outEdges(node)) {
        N successorNode = network.incidentNodes(edge).adjacentNode(node);
        if (subgraph.nodes().contains(successorNode)) {
          subgraph.addEdge(node, successorNode, edge);
        }
      }
    }
    return subgraph;
  }

  /** Creates a mutable copy of {@code graph} with the same nodes and edges. */
  public static <N> MutableGraph<N> copyOf(Graph<N> graph) {
    MutableGraph<N> copy = GraphBuilder.from(graph).expectedNodeCount(graph.nodes().size()).build();
    for (N node : graph.nodes()) {
      copy.addNode(node);
    }
    for (EndpointPair<N> edge : graph.edges()) {
      copy.putEdge(edge.nodeU(), edge.nodeV());
    }
    return copy;
  }

  /** Creates a mutable copy of {@code graph} with the same nodes, edges, and edge values. */
  public static <N, V> MutableValueGraph<N, V> copyOf(ValueGraph<N, V> graph) {
    MutableValueGraph<N, V> copy =
        ValueGraphBuilder.from(graph).expectedNodeCount(graph.nodes().size()).build();
    for (N node : graph.nodes()) {
      copy.addNode(node);
    }
    for (EndpointPair<N> edge : graph.edges()) {
      copy.putEdgeValue(edge.nodeU(), edge.nodeV(), graph.edgeValue(edge.nodeU(), edge.nodeV()));
    }
    return copy;
  }

  /** Creates a mutable copy of {@code network} with the same nodes and edges. */
  public static <N, E> MutableNetwork<N, E> copyOf(Network<N, E> network) {
    MutableNetwork<N, E> copy =
        NetworkBuilder.from(network)
            .expectedNodeCount(network.nodes().size())
            .expectedEdgeCount(network.edges().size())
            .build();
    for (N node : network.nodes()) {
      copy.addNode(node);
    }
    for (E edge : network.edges()) {
      EndpointPair<N> endpointPair = network.incidentNodes(edge);
      copy.addEdge(endpointPair.nodeU(), endpointPair.nodeV(), edge);
    }
    return copy;
  }

  @CanIgnoreReturnValue
  static int checkNonNegative(int value) {
    checkArgument(value >= 0, "Not true that %s is non-negative.", value);
    return value;
  }

  @CanIgnoreReturnValue
  static int checkPositive(int value) {
    checkArgument(value > 0, "Not true that %s is positive.", value);
    return value;
  }

  @CanIgnoreReturnValue
  static long checkNonNegative(long value) {
    checkArgument(value >= 0, "Not true that %s is non-negative.", value);
    return value;
  }

  @CanIgnoreReturnValue
  static long checkPositive(long value) {
    checkArgument(value > 0, "Not true that %s is positive.", value);
    return value;
  }

  /**
   * An enum representing the state of a node during DFS. {@code PENDING} means that the node is on
   * the stack of the DFS, while {@code COMPLETE} means that the node and all its successors have
   * been already explored. Any node that has not been explored will not have a state at all.
   */
  private enum NodeVisitState {
    PENDING,
    COMPLETE
  }
}
