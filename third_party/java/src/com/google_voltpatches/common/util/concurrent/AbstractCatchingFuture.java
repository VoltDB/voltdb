/*
 * Copyright (C) 2006 The Guava Authors
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
import static com.google_voltpatches.common.util.concurrent.Futures.getDone;
import static com.google_voltpatches.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google_voltpatches.common.util.concurrent.MoreExecutors.rejectionPropagatingExecutor;
import static com.google_voltpatches.common.util.concurrent.Platform.isInstanceOfThrowableClass;

import com.google_voltpatches.common.annotations.GwtCompatible;
import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.errorprone.annotations.ForOverride;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import javax.annotation_voltpatches.Nullable;

/**
 * Implementations of {@code Futures.catching*}.
 */
@GwtCompatible
abstract class AbstractCatchingFuture<V, X extends Throwable, F, T>
    extends AbstractFuture.TrustedFuture<V> implements Runnable {
  static <X extends Throwable, V> ListenableFuture<V> create(
      ListenableFuture<? extends V> input,
      Class<X> exceptionType,
      Function<? super X, ? extends V> fallback) {
    CatchingFuture<V, X> future = new CatchingFuture<V, X>(input, exceptionType, fallback);
    input.addListener(future, directExecutor());
    return future;
  }

  static <V, X extends Throwable> ListenableFuture<V> create(
      ListenableFuture<? extends V> input,
      Class<X> exceptionType,
      Function<? super X, ? extends V> fallback,
      Executor executor) {
    CatchingFuture<V, X> future = new CatchingFuture<V, X>(input, exceptionType, fallback);
    input.addListener(future, rejectionPropagatingExecutor(executor, future));
    return future;
  }

  static <X extends Throwable, V> ListenableFuture<V> create(
      ListenableFuture<? extends V> input,
      Class<X> exceptionType,
      AsyncFunction<? super X, ? extends V> fallback) {
    AsyncCatchingFuture<V, X> future =
        new AsyncCatchingFuture<V, X>(input, exceptionType, fallback);
    input.addListener(future, directExecutor());
    return future;
  }

  static <X extends Throwable, V> ListenableFuture<V> create(
      ListenableFuture<? extends V> input,
      Class<X> exceptionType,
      AsyncFunction<? super X, ? extends V> fallback,
      Executor executor) {
    AsyncCatchingFuture<V, X> future =
        new AsyncCatchingFuture<V, X>(input, exceptionType, fallback);
    input.addListener(future, rejectionPropagatingExecutor(executor, future));
    return future;
  }

  /*
   * In certain circumstances, this field might theoretically not be visible to an afterDone() call
   * triggered by cancel(). For details, see the comments on the fields of TimeoutFuture.
   */
  @Nullable ListenableFuture<? extends V> inputFuture;
  @Nullable Class<X> exceptionType;
  @Nullable F fallback;

  AbstractCatchingFuture(
      ListenableFuture<? extends V> inputFuture, Class<X> exceptionType, F fallback) {
    this.inputFuture = checkNotNull(inputFuture);
    this.exceptionType = checkNotNull(exceptionType);
    this.fallback = checkNotNull(fallback);
  }

  @Override
  public final void run() {
    ListenableFuture<? extends V> localInputFuture = inputFuture;
    Class<X> localExceptionType = exceptionType;
    F localFallback = fallback;
    if (localInputFuture == null
        | localExceptionType == null
        | localFallback == null
        | isCancelled()) {
      return;
    }
    inputFuture = null;
    exceptionType = null;
    fallback = null;

    // For an explanation of the cases here, see the comments on AbstractTransformFuture.run.
    V sourceResult = null;
    Throwable throwable = null;
    try {
      sourceResult = getDone(localInputFuture);
    } catch (ExecutionException e) {
      throwable = checkNotNull(e.getCause());
    } catch (Throwable e) { // this includes cancellation exception
      throwable = e;
    }

    if (throwable == null) {
      set(sourceResult);
      return;
    }

    if (!isInstanceOfThrowableClass(throwable, localExceptionType)) {
      setException(throwable);
      // TODO(cpovirk): Test that fallback is not run in this case.
      return;
    }

    @SuppressWarnings("unchecked") // verified safe by isInstanceOfThrowableClass
    X castThrowable = (X) throwable;
    T fallbackResult;
    try {
      fallbackResult = doFallback(localFallback, castThrowable);
    } catch (Throwable t) {
      setException(t);
      return;
    }

    setResult(fallbackResult);
  }

  /** Template method for subtypes to actually run the fallback. */
  @ForOverride
  @Nullable
  abstract T doFallback(F fallback, X throwable) throws Exception;

  /** Template method for subtypes to actually set the result. */
  @ForOverride
  abstract void setResult(@Nullable T result);

  @Override
  protected final void afterDone() {
    maybePropagateCancellation(inputFuture);
    this.inputFuture = null;
    this.exceptionType = null;
    this.fallback = null;
  }

  /**
   * An {@link AbstractCatchingFuture} that delegates to an {@link AsyncFunction} and
   * {@link #setFuture(ListenableFuture)}.
   */
  private static final class AsyncCatchingFuture<V, X extends Throwable>
      extends AbstractCatchingFuture<
          V, X, AsyncFunction<? super X, ? extends V>, ListenableFuture<? extends V>> {
    AsyncCatchingFuture(
        ListenableFuture<? extends V> input,
        Class<X> exceptionType,
        AsyncFunction<? super X, ? extends V> fallback) {
      super(input, exceptionType, fallback);
    }

    @Override
    ListenableFuture<? extends V> doFallback(
        AsyncFunction<? super X, ? extends V> fallback, X cause) throws Exception {
      ListenableFuture<? extends V> replacement = fallback.apply(cause);
      checkNotNull(
          replacement,
          "AsyncFunction.apply returned null instead of a Future. "
              + "Did you mean to return immediateFuture(null)?");
      return replacement;
    }

    @Override
    void setResult(ListenableFuture<? extends V> result) {
      setFuture(result);
    }
  }

  /**
   * An {@link AbstractCatchingFuture} that delegates to a {@link Function} and {@link
   * #set(Object)}.
   */
  private static final class CatchingFuture<V, X extends Throwable>
      extends AbstractCatchingFuture<V, X, Function<? super X, ? extends V>, V> {
    CatchingFuture(
        ListenableFuture<? extends V> input,
        Class<X> exceptionType,
        Function<? super X, ? extends V> fallback) {
      super(input, exceptionType, fallback);
    }

    @Override
    @Nullable
    V doFallback(Function<? super X, ? extends V> fallback, X cause) throws Exception {
      return fallback.apply(cause);
    }

    @Override
    void setResult(@Nullable V result) {
      set(result);
    }
  }
}
