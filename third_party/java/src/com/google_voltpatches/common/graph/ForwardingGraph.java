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

import java.util.Set;

/**
 * A class to allow {@link Graph} implementations to be backed by {@link ValueGraph}s. This is not
 * currently planned to be released as a general-purpose forwarding class.
 *
 * @author James Sexton
 */
abstract class ForwardingGraph<N> extends AbstractGraph<N> {

  protected abstract Graph<N> delegate();

  @Override
  public Set<N> nodes() {
    return delegate().nodes();
  }

  @Override
  public Set<EndpointPair<N>> edges() {
    return delegate().edges();
  }

  @Override
  public boolean isDirected() {
    return delegate().isDirected();
  }

  @Override
  public boolean allowsSelfLoops() {
    return delegate().allowsSelfLoops();
  }

  @Override
  public ElementOrder<N> nodeOrder() {
    return delegate().nodeOrder();
  }

  @Override
  public Set<N> adjacentNodes(Object node) {
    return delegate().adjacentNodes(node);
  }

  @Override
  public Set<N> predecessors(Object node) {
    return delegate().predecessors(node);
  }

  @Override
  public Set<N> successors(Object node) {
    return delegate().successors(node);
  }

  @Override
  public int degree(Object node) {
    return delegate().degree(node);
  }

  @Override
  public int inDegree(Object node) {
    return delegate().inDegree(node);
  }

  @Override
  public int outDegree(Object node) {
    return delegate().outDegree(node);
  }
}
