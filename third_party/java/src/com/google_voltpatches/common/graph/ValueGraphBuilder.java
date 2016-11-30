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
import static com.google_voltpatches.common.graph.Graphs.checkNonNegative;

import com.google_voltpatches.common.annotations.Beta;
import com.google_voltpatches.common.base.Optional;

/**
 * A builder for constructing instances of {@link MutableValueGraph} with user-defined properties.
 *
 * <p>A graph built by this class will have the following properties by default:
 *
 * <ul>
 * <li>does not allow self-loops
 * <li>orders {@link Graph#nodes()} in the order in which the elements were added
 * </ul>
 *
 * <p>Example of use:
 *
 * <pre>{@code
 * MutableValueGraph<String, Double> graph =
 *     ValueGraphBuilder.undirected().allowsSelfLoops(true).build();
 * graph.putEdgeValue("San Francisco", "San Francisco", 0.0);
 * graph.putEdgeValue("San Jose", "San Jose", 0.0);
 * graph.putEdgeValue("San Francisco", "San Jose", 48.4);
 * }</pre>
 *
 * @author James Sexton
 * @author Joshua O'Madadhain
 * @since 20.0
 */
@Beta
public final class ValueGraphBuilder<N, V> extends AbstractGraphBuilder<N> {

  /** Creates a new instance with the specified edge directionality. */
  private ValueGraphBuilder(boolean directed) {
    super(directed);
  }

  /** Returns a {@link ValueGraphBuilder} for building directed graphs. */
  public static ValueGraphBuilder<Object, Object> directed() {
    return new ValueGraphBuilder<Object, Object>(true);
  }

  /** Returns a {@link ValueGraphBuilder} for building undirected graphs. */
  public static ValueGraphBuilder<Object, Object> undirected() {
    return new ValueGraphBuilder<Object, Object>(false);
  }

  /**
   * Returns a {@link ValueGraphBuilder} initialized with all properties queryable from {@code
   * graph}.
   *
   * <p>The "queryable" properties are those that are exposed through the {@link Graph} interface,
   * such as {@link Graph#isDirected()}. Other properties, such as {@link #expectedNodeCount(int)},
   * are not set in the new builder.
   */
  public static <N> ValueGraphBuilder<N, Object> from(Graph<N> graph) {
    return new ValueGraphBuilder<N, Object>(graph.isDirected())
        .allowsSelfLoops(graph.allowsSelfLoops())
        .nodeOrder(graph.nodeOrder());
  }

  /**
   * Specifies whether the graph will allow self-loops (edges that connect a node to itself).
   * Attempting to add a self-loop to a graph that does not allow them will throw an {@link
   * UnsupportedOperationException}.
   */
  public ValueGraphBuilder<N, V> allowsSelfLoops(boolean allowsSelfLoops) {
    this.allowsSelfLoops = allowsSelfLoops;
    return this;
  }

  /**
   * Specifies the expected number of nodes in the graph.
   *
   * @throws IllegalArgumentException if {@code expectedNodeCount} is negative
   */
  public ValueGraphBuilder<N, V> expectedNodeCount(int expectedNodeCount) {
    this.expectedNodeCount = Optional.of(checkNonNegative(expectedNodeCount));
    return this;
  }

  /** Specifies the order of iteration for the elements of {@link Graph#nodes()}. */
  public <N1 extends N> ValueGraphBuilder<N1, V> nodeOrder(ElementOrder<N1> nodeOrder) {
    ValueGraphBuilder<N1, V> newBuilder = cast();
    newBuilder.nodeOrder = checkNotNull(nodeOrder);
    return newBuilder;
  }

  /**
   * Returns an empty {@link MutableValueGraph} with the properties of this {@link
   * ValueGraphBuilder}.
   */
  public <N1 extends N, V1 extends V> MutableValueGraph<N1, V1> build() {
    return new ConfigurableMutableValueGraph<N1, V1>(this);
  }

  @SuppressWarnings("unchecked")
  private <N1 extends N, V1 extends V> ValueGraphBuilder<N1, V1> cast() {
    return (ValueGraphBuilder<N1, V1>) this;
  }
}
