/*
 * Copyright (C) 2006 The Guava Authors
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

package com.google_voltpatches.common.util.concurrent;

import static com.google_voltpatches.common.base.Preconditions.checkNotNull;
import static com.google_voltpatches.common.base.Preconditions.checkState;
import static com.google_voltpatches.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google_voltpatches.common.util.concurrent.Uninterruptibles.getUninterruptibly;

import com.google_voltpatches.common.annotations.GwtCompatible;
import com.google_voltpatches.common.annotations.GwtIncompatible;
import com.google_voltpatches.common.collect.ImmutableCollection;
import com.google_voltpatches.j2objc.annotations.WeakOuter;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation_voltpatches.Nullable;

/**
 * A future made up of a collection of sub-futures.
 *
 * @param <InputT> the type of the individual inputs
 * @param <OutputT> the type of the output (i.e. this) future
 */
@GwtCompatible
abstract class AggregateFuture<InputT, OutputT> extends AbstractFuture.TrustedFuture<OutputT> {
  private static final Logger logger =
      Logger.getLogger(AggregateFuture.class.getName());

  private RunningState runningState;

  @Override final void done() {
    super.done();

    // Let go of the memory held by the running state
    this.runningState = null;
  }

  // TODO(cpovirk): Use maybePropagateCancellation() if the performance is OK and the code is clean.
  @Override public final boolean cancel(boolean mayInterruptIfRunning) {
    // Must get a reference to the futures before we cancel, as they'll be cleared out.
    RunningState localRunningState = runningState;
    ImmutableCollection<? extends ListenableFuture<? extends InputT>> futures =
        (localRunningState != null) ? localRunningState.futures : null;
    // Cancel all the component futures.
    boolean cancelled = super.cancel(mayInterruptIfRunning);
    // & is faster than the branch required for &&
    if (cancelled & futures != null) {
      for (ListenableFuture<?> future : futures) {
        future.cancel(mayInterruptIfRunning);
      }
    }
    return cancelled;
  }

  @GwtIncompatible("Interruption not supported")
  @Override protected final void interruptTask() {
    RunningState localRunningState = runningState;
    if (localRunningState != null) {
      localRunningState.interruptTask();
    }
  }

  /**
   * Must be called at the end of each sub-class's constructor.
   */
  final void init(RunningState runningState) {
    this.runningState = runningState;
    runningState.init();
  }

  @WeakOuter
  abstract class RunningState extends AggregateFutureState implements Runnable {
    private ImmutableCollection<? extends ListenableFuture<? extends InputT>> futures;
    private final boolean allMustSucceed;
    private final boolean collectsValues;

    RunningState(ImmutableCollection<? extends ListenableFuture<? extends InputT>> futures,
        boolean allMustSucceed, boolean collectsValues) {
      super(futures.size());
      this.futures = checkNotNull(futures);
      this.allMustSucceed = allMustSucceed;
      this.collectsValues = collectsValues;
    }

    /* Used in the !allMustSucceed case so we don't have to instantiate a listener. */
    @Override public final void run() {
      decrementCountAndMaybeComplete();
    }

    /**
     * The "real" initialization; we can't put this in the constructor because, in the case where
     * futures are already complete, we would not initialize the subclass before calling
     * {@link #handleOneInputDone}. As this is called after the subclass is constructed, we're
     * guaranteed to have properly initialized the subclass.
     */
    private void init() {
      // Corner case: List is empty.
      if (futures.isEmpty()) {
        handleAllCompleted();
        return;
      }

      // NOTE: If we ever want to use a custom executor here, have a look at
      // CombinedFuture as we'll need to handle RejectedExecutionException

      if (allMustSucceed) {
        // We need fail fast, so we have to keep track of which future failed so we can propagate
        // the exception immediately

        // Register a listener on each Future in the list to update
        // the state of this future.
        // Note that if all the futures on the list are done prior to completing
        // this loop, the last call to addListener() will callback to
        // setOneValue(), transitively call our cleanup listener, and set
        // this.futures to null.
        // This is not actually a problem, since the foreach only needs
        // this.futures to be non-null at the beginning of the loop.
        int i = 0;
        for (final ListenableFuture<? extends InputT> listenable : futures) {
          final int index = i++;
          listenable.addListener(new Runnable() {
            @Override
            public void run() {
              try {
                handleOneInputDone(index, listenable);
              } finally {
                decrementCountAndMaybeComplete();
              }
            }
          }, directExecutor());
        }
      } else {
        // We'll only call the callback when all futures complete, regardless of whether some failed
        // Hold off on calling setOneValue until all complete, so we can share the same listener
        for (ListenableFuture<? extends InputT> listenable : futures) {
          listenable.addListener(this, directExecutor());
        }
      }
    }

    /**
     * Fails this future with the given Throwable if {@link #allMustSucceed} is
     * true. Also, logs the throwable if it is an {@link Error} or if
     * {@link #allMustSucceed} is {@code true}, the throwable did not cause
     * this future to fail, and it is the first time we've seen that particular Throwable.
     */
    private void handleException(Throwable throwable) {
      checkNotNull(throwable);

      boolean completedWithFailure = false;
      boolean firstTimeSeeingThisException = true;
      if (allMustSucceed) {
        // As soon as the first one fails, throw the exception up.
        // The result of all other inputs is then ignored.
        completedWithFailure = setException(throwable);
        if (completedWithFailure) {
          releaseResourcesAfterFailure();
        } else {
          // Go up the causal chain to see if we've already seen this cause; if we have,
          // even if it's wrapped by a different exception, don't log it.
          firstTimeSeeingThisException = addCausalChain(getOrInitSeenExceptions(), throwable);
        }
      }

      // | and & used because it's faster than the branch required for || and &&
      if (throwable instanceof Error
          | (allMustSucceed & !completedWithFailure & firstTimeSeeingThisException)) {
        String message =
            (throwable instanceof Error)
                ? "Input Future failed with Error"
                : "Got more than one input Future failure. Logging failures after the first";
        logger.log(Level.SEVERE, message, throwable);
      }
    }

    @Override
    final void addInitialException(Set<Throwable> seen) {
      if (!isCancelled()) {
        addCausalChain(seen, trustedGetException());
      }
    }

    /**
     * Handles the input at the given index completing.
     */
    private void handleOneInputDone(int index, Future<? extends InputT> future) {
      // The only cases in which this Future should already be done are (a) if
      // it was cancelled or (b) if an input failed and we propagated that
      // immediately because of allMustSucceed.
      checkState(allMustSucceed || !isDone() || isCancelled(),
          "Future was done before all dependencies completed");

      try {
        checkState(future.isDone(),
            "Tried to set value from future which is not done");
        if (allMustSucceed) {
          if (future.isCancelled()) {
            // this.cancel propagates the cancellation to children; we use super.cancel
            // to set our own state but let the input futures keep running
            // as some of them may be used elsewhere.
            AggregateFuture.super.cancel(false);
          } else {
            // We always get the result so that we can have fail-fast, even if we don't collect
            InputT result = getUninterruptibly(future);
            if (collectsValues) {
              collectOneValue(allMustSucceed, index, result);
            }
          }
        } else if (collectsValues && !future.isCancelled()) {
          collectOneValue(allMustSucceed, index, getUninterruptibly(future));
        }
      } catch (ExecutionException e) {
        handleException(e.getCause());
      } catch (Throwable t) {
        handleException(t);
      }
    }

    private void decrementCountAndMaybeComplete() {
      int newRemaining = decrementRemainingAndGet();
      checkState(newRemaining >= 0, "Less than 0 remaining futures");
      if (newRemaining == 0) {
        processCompleted();
      }
    }

    private void processCompleted() {
      // Collect the values if (a) our output requires collecting them and (b) we haven't been
      // collecting them as we go. (We've collected them as we go only if we needed to fail fast)
      if (collectsValues & !allMustSucceed) {
        int i = 0;
        for (ListenableFuture<? extends InputT> listenable : futures) {
          handleOneInputDone(i++, listenable);
        }
      }
      handleAllCompleted();
    }

    /**
     * Listeners implicitly keep a reference to {@link RunningState} as they're inner classes,
     * so we free resources here as well for the allMustSucceed=true case (i.e. when a future fails,
     * we immediately release resources we no longer need); additionally, the future will release
     * its reference to {@link RunningState}, which should free all associated memory when all the
     * futures complete & the listeners are released.
     *
     * TODO(user): Write tests for memory retention
     */
    void releaseResourcesAfterFailure() {
      this.futures = null;
    }

    /**
     * Called only if {@code collectsValues} is true.
     *
     * <p>If {@code allMustSucceed} is true, called as each future completes; otherwise,
     * called for each future when all futures complete.
     */
    abstract void collectOneValue(boolean allMustSucceed, int index, @Nullable InputT returnValue);

    abstract void handleAllCompleted();

    void interruptTask() {}
  }

  /** Adds the chain to the seen set, and returns whether all the chain was new to us. */
  private static boolean addCausalChain(Set<Throwable> seen, Throwable t) {
    for (; t != null; t = t.getCause()) {
      boolean firstTimeSeen = seen.add(t);
      if (!firstTimeSeen) {
        /*
         * We've seen this, so we've seen its causes, too. No need to re-add them. (There's one case
         * where this isn't true, but we ignore it: If we record an exception, then someone calls
         * initCause() on it, and then we examine it again, we'll conclude that we've seen the whole
         * chain before when it fact we haven't. But this should be rare.)
         */
        return false;
      }
    }
    return true;
  }
}
