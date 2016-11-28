/*
 * Copyright (C) 2014 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google_voltpatches.common.util.concurrent;

import static com.google_voltpatches.common.base.Preconditions.checkNotNull;

import com.google_voltpatches.common.annotations.GwtCompatible;
import com.google_voltpatches.j2objc.annotations.WeakOuter;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import javax.annotation_voltpatches.Nullable;

/**
 * A {@link RunnableFuture} that also implements the {@link ListenableFuture} interface.
 * 
 * <p>This should be used in preference to {@link ListenableFutureTask} when possible for 
 * performance reasons.
 */
@GwtCompatible
class TrustedListenableFutureTask<V> extends AbstractFuture.TrustedFuture<V>
    implements RunnableFuture<V> {

  /**
   * Creates a {@code ListenableFutureTask} that will upon running, execute the given
   * {@code Callable}.
   *
   * @param callable the callable task
   */
  static <V> TrustedListenableFutureTask<V> create(Callable<V> callable) {
    return new TrustedListenableFutureTask<V>(callable);
  }

  /**
   * Creates a {@code ListenableFutureTask} that will upon running, execute the given
   * {@code Runnable}, and arrange that {@code get} will return the given result on successful
   * completion.
   *
   * @param runnable the runnable task
   * @param result the result to return on successful completion. If you don't need a particular
   *     result, consider using constructions of the form:
   *     {@code ListenableFuture<?> f = ListenableFutureTask.create(runnable,
   *     null)}
   */
  static <V> TrustedListenableFutureTask<V> create(Runnable runnable, @Nullable V result) {
    return new TrustedListenableFutureTask<V>(Executors.callable(runnable, result));
  }

  /*
   * In certain circumstances, this field might theoretically not be visible to an afterDone() call
   * triggered by cancel(). For details, see the comments on the fields of TimeoutFuture.
   */
  private TrustedFutureInterruptibleTask task;

  TrustedListenableFutureTask(Callable<V> callable) {
    this.task = new TrustedFutureInterruptibleTask(callable);
  }

  @Override
  public void run() {
    TrustedFutureInterruptibleTask localTask = task;
    if (localTask != null) {
      localTask.run();
    }
  }

  @Override
  protected void afterDone() {
    super.afterDone();

    if (wasInterrupted()) {
      TrustedFutureInterruptibleTask localTask = task;
      if (localTask != null) {
        localTask.interruptTask();
      }
    }

    this.task = null;
  }

  @Override
  public String toString() {
    return super.toString() + " (delegate = " + task + ")";
  }

  @WeakOuter
  private final class TrustedFutureInterruptibleTask extends InterruptibleTask {
    private final Callable<V> callable;

    TrustedFutureInterruptibleTask(Callable<V> callable) {
      this.callable = checkNotNull(callable);
    }

    @Override
    void runInterruptibly() {
      // Ensure we haven't been cancelled or already run.
      if (!isDone()) {
        try {
          set(callable.call());
        } catch (Throwable t) {
          setException(t);
        } 
      }
    }

    @Override
    boolean wasInterrupted() {
      return TrustedListenableFutureTask.this.wasInterrupted();
    }

    @Override
    public String toString() {
      return callable.toString();
    }
  }
}
