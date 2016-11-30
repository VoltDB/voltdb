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

import com.google_voltpatches.common.annotations.Beta;
import java.util.Set;
import javax.annotation_voltpatches.Nullable;

/**
 * An interface for <a
 * href="https://en.wikipedia.org/wiki/Graph_(discrete_mathematics)">graph</a>-structured data,
 * whose edges are anonymous entities with no identity or information of their own.
 *
 * <p>A graph is composed of a set of nodes and a set of edges connecting pairs of nodes.
 *
 * <p>There are three main interfaces provided to represent graphs. In order of increasing
 * complexity they are: {@link Graph}, {@link ValueGraph}, and {@link Network}. You should generally
 * prefer the simplest interface that satisfies your use case. See the <a
 * href="https://github.com/google/guava/wiki/GraphsExplained#choosing-the-right-graph-type">
 * "Choosing the right graph type"</a> section of the Guava User Guide for more details.
 *
 * <h3>Capabilities</h3>
 *
 * <p>{@code Graph} supports the following use cases (<a
 * href="https://github.com/google/guava/wiki/GraphsExplained#definitions">definitions of
 * terms</a>):
 *
 * <ul>
 *   <li>directed graphs
 *   <li>undirected graphs
 *   <li>graphs that do/don't allow self-loops
 *   <li>graphs whose nodes/edges are insertion-ordered, sorted, or unordered
 * </ul>
 *
 * <p>{@code Graph} explicitly does not support parallel edges, and forbids implementations or
 * extensions with parallel edges. If you need parallel edges, use {@link Network}.
 *
 * <h3>Building a {@code Graph}</h3>
 *
 * <p>The implementation classes that `common.graph` provides are not public, by design. To create
 * an instance of one of the built-in implementations of {@code Graph}, use the {@link GraphBuilder}
 * class:
 *
 * <pre>{@code
 *   MutableGraph<Integer> graph = GraphBuilder.undirected().build();
 * }</pre>
 *
 * <p>{@link GraphBuilder#build()} returns an instance of {@link MutableGraph}, which is a subtype
 * of {@code Graph} that provides methods for adding and removing nodes and edges. If you do not
 * need to mutate a graph (e.g. if you write a method than runs a read-only algorithm on the graph),
 * you should use the non-mutating {@link Graph} interface, or an {@link ImmutableGraph}.
 *
 * <p>You can create an immutable copy of an existing {@code Graph} using {@link
 * ImmutableGraph#copyOf(Graph)}:
 *
 * <pre>{@code
 *   ImmutableGraph<Integer> immutableGraph = ImmutableGraph.copyOf(graph);
 * }</pre>
 *
 * <p>Instances of {@link ImmutableGraph} do not implement {@link MutableGraph} (obviously!) and are
 * contractually guaranteed to be unmodifiable and thread-safe.
 *
 * <p>The Guava User Guide has <a
 * href="https://github.com/google/guava/wiki/GraphsExplained#building-graph-instances">more
 * information on (and examples of) building graphs</a>.
 *
 * <h3>Additional documentation</h3>
 *
 * <p>See the Guava User Guide for the {@code common.graph} package (<a
 * href="https://github.com/google/guava/wiki/GraphsExplained">"Graphs Explained"</a>) for
 * additional documentation, including:
 *
 * <ul>
 *   <li><a
 *       href="https://github.com/google/guava/wiki/GraphsExplained#equals-hashcode-and-graph-equivalence">
 *       {@code equals()}, {@code hashCode()}, and graph equivalence</a>
 *   <li><a href="https://github.com/google/guava/wiki/GraphsExplained#synchronization">
 *       Synchronization policy</a>
 *   <li><a href="https://github.com/google/guava/wiki/GraphsExplained#notes-for-implementors">Notes
 *       for implementors</a>
 * </ul>
 *
 * @author James Sexton
 * @author Joshua O'Madadhain
 * @param <N> Node parameter type
 * @since 20.0
 */
@Beta
public interface Graph<N> {
  //
  // Graph-level accessors
  //

  /** Returns all nodes in this graph, in the order specified by {@link #nodeOrder()}. */
  Set<N> nodes();

  /** Returns all edges in this graph. */
  Set<EndpointPair<N>> edges();

  //
  // Graph properties
  //

  /**
   * Returns true if the edges in this graph are directed. Directed edges connect a {@link
   * EndpointPair#source() source node} to a {@link EndpointPair#target() target node}, while
   * undirected edges connect a pair of nodes to each other.
   */
  boolean isDirected();

  /**
   * Returns true if this graph allows self-loops (edges that connect a node to itself). Attempting
   * to add a self-loop to a graph that does not allow them will throw an {@link
   * UnsupportedOperationException}.
   */
  boolean allowsSelfLoops();

  /** Returns the order of iteration for the elements of {@link #nodes()}. */
  ElementOrder<N> nodeOrder();

  //
  // Element-level accessors
  //

  /**
   * Returns the nodes which have an incident edge in common with {@code node} in this graph.
   *
   * @throws IllegalArgumentException if {@code node} is not an element of this graph
   */
  Set<N> adjacentNodes(Object node);

  /**
   * Returns all nodes in this graph adjacent to {@code node} which can be reached by traversing
   * {@code node}'s incoming edges <i>against</i> the direction (if any) of the edge.
   *
   * <p>In an undirected graph, this is equivalent to {@link #adjacentNodes(Object)}.
   *
   * @throws IllegalArgumentException if {@code node} is not an element of this graph
   */
  Set<N> predecessors(Object node);

  /**
   * Returns all nodes in this graph adjacent to {@code node} which can be reached by traversing
   * {@code node}'s outgoing edges in the direction (if any) of the edge.
   *
   * <p>In an undirected graph, this is equivalent to {@link #adjacentNodes(Object)}.
   *
   * <p>This is <i>not</i> the same as "all nodes reachable from {@code node} by following outgoing
   * edges". For that functionality, see {@link Graphs#reachableNodes(Graph, Object)}.
   *
   * @throws IllegalArgumentException if {@code node} is not an element of this graph
   */
  Set<N> successors(Object node);

  /**
   * Returns the count of {@code node}'s incident edges, counting self-loops twice (equivalently,
   * the number of times an edge touches {@code node}).
   *
   * <p>For directed graphs, this is equal to {@code inDegree(node) + outDegree(node)}.
   *
   * <p>For undirected graphs, this is equal to {@code adjacentNodes(node).size()} + (1 if {@code
   * node} has an incident self-loop, 0 otherwise).
   *
   * <p>If the count is greater than {@code Integer.MAX_VALUE}, returns {@code Integer.MAX_VALUE}.
   *
   * @throws IllegalArgumentException if {@code node} is not an element of this graph
   */
  int degree(Object node);

  /**
   * Returns the count of {@code node}'s incoming edges (equal to {@code predecessors(node).size()})
   * in a directed graph. In an undirected graph, returns the {@link #degree(Object)}.
   *
   * <p>If the count is greater than {@code Integer.MAX_VALUE}, returns {@code Integer.MAX_VALUE}.
   *
   * @throws IllegalArgumentException if {@code node} is not an element of this graph
   */
  int inDegree(Object node);

  /**
   * Returns the count of {@code node}'s outgoing edges (equal to {@code successors(node).size()})
   * in a directed graph. In an undirected graph, returns the {@link #degree(Object)}.
   *
   * <p>If the count is greater than {@code Integer.MAX_VALUE}, returns {@code Integer.MAX_VALUE}.
   *
   * @throws IllegalArgumentException if {@code node} is not an element of this graph
   */
  int outDegree(Object node);

  //
  // Graph identity
  //

  /**
   * For the default {@link Graph} implementations, returns true if {@code this == object}
   * (reference equality). External implementations are free to define this method as they see fit,
   * as long as they satisfy the {@link Object#equals(Object)} contract.
   *
   * <p>To compare two {@link Graph}s based on their contents rather than their references, see
   * {@link Graphs#equivalent(Graph, Graph)}.
   */
  @Override
  boolean equals(@Nullable Object object);

  /**
   * For the default {@link Graph} implementations, returns {@code System.identityHashCode(this)}.
   * External implementations are free to define this method as they see fit, as long as they
   * satisfy the {@link Object#hashCode()} contract.
   */
  @Override
  int hashCode();
}
