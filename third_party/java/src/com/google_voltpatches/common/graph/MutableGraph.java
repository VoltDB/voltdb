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
import com.google_voltpatches.errorprone.annotations.CanIgnoreReturnValue;

/**
 * A subinterface of {@link Graph} which adds mutation methods. When mutation is not required, users
 * should prefer the {@link Graph} interface.
 *
 * @author James Sexton
 * @author Joshua O'Madadhain
 * @param <N> Node parameter type
 * @since 20.0
 */
@Beta
public interface MutableGraph<N> extends Graph<N> {

  /**
   * Adds {@code node} if it is not already present.
   *
   * <p><b>Nodes must be unique</b>, just as {@code Map} keys must be. They must also be non-null.
   *
   * @return {@code true} if the graph was modified as a result of this call
   */
  @CanIgnoreReturnValue
  boolean addNode(N node);

  /**
   * Adds an edge connecting {@code nodeU} to {@code nodeV} if one is not already present. In an
   * undirected graph, the edge will also connect {@code nodeV} to {@code nodeU}.
   *
   * <p>Behavior if {@code nodeU} and {@code nodeV} are not already present in this graph is
   * implementation-dependent. Suggested behaviors include (a) silently {@link #addNode(Object)
   * adding} {@code nodeU} and {@code nodeV} to the graph (this is the behavior of the default
   * implementations) or (b) throwing {@code IllegalArgumentException}.
   *
   * @return {@code true} if the graph was modified as a result of this call
   * @throws IllegalArgumentException if the introduction of the edge would violate {@link
   *     #allowsSelfLoops()}
   */
  @CanIgnoreReturnValue
  boolean putEdge(N nodeU, N nodeV);

  /**
   * Removes {@code node} if it is present; all edges incident to {@code node} will also be removed.
   *
   * @return {@code true} if the graph was modified as a result of this call
   */
  @CanIgnoreReturnValue
  boolean removeNode(Object node);

  /**
   * Removes the edge connecting {@code nodeU} to {@code nodeV}, if it is present.
   *
   * @return {@code true} if the graph was modified as a result of this call
   */
  @CanIgnoreReturnValue
  boolean removeEdge(Object nodeU, Object nodeV);
}
