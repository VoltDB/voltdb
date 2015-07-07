/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.importer;

import static com.google_voltpatches.common.base.Preconditions.checkNotNull;
import static com.google_voltpatches.common.base.Predicates.equalTo;
import static com.google_voltpatches.common.base.Predicates.not;

import java.net.URI;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;

import com.google_voltpatches.common.base.Optional;
import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.ImmutableSetMultimap;
import com.google_voltpatches.common.collect.ImmutableSortedMap;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.SetMultimap;
import com.google_voltpatches.common.collect.Sets;

public class ChannelChangeNotifier implements Runnable {

    private final static VoltLogger LOG = new VoltLogger("IMPORT");

    /**
     * Boiler plate method to log an error message and wrap, and return a {@link DistributerException}
     * around the message and cause
     *
     * @param cause fault origin {@link Throwable}
     * @param format a {@link String#format(String, Object...) compliant format string
     * @param args formatter arguments
     * @return a {@link DistributerException}
     */
    static DistributerException loggedDistributerException(Throwable cause, String format, Object...args) {
        Optional<DistributerException> causeFor = DistributerException.isCauseFor(cause);
        if (causeFor.isPresent()) {
            return causeFor.get();
        }
        String msg = String.format(format, args);
        if (cause != null) {
            LOG.error(msg, cause);
            return new DistributerException(msg, cause);
        } else {
            LOG.error(msg);
            return new DistributerException(msg);
        }
    }

    SetMultimap<String, URI> mapByImporter(Set<ChannelSpec> specs) {
        ImmutableSetMultimap.Builder<String, URI> mmbldr = ImmutableSetMultimap.builder();
        for (ChannelSpec spec: specs) {
            mmbldr.put(spec.getImporter(),spec.getUri());
        }
        return mmbldr.build();
    }

    private final CallbacksRef m_callbacks = new CallbacksRef();
    private final AtomicReference<BlockingDeque<ChannelAssignment>> m_qref = new AtomicReference<>();
    private final AtomicBoolean m_done = new AtomicBoolean(false);
    private final ExecutorService m_es;

    public ChannelChangeNotifier() {
        m_es = CoreUtils.getCachedSingleThreadExecutor("Import Channel Change Notification Dispatcher", 15000);
    }

    public void startPolling(BlockingDeque<ChannelAssignment> deque) {
        if (m_qref.compareAndSet(null, checkNotNull(deque, "deque is null"))) {
            m_es.submit(this);
        } else {
            throw new IllegalStateException("this notifier has already an assigned blocking deque");
        }
    }

    public void registerCallback(String importer, ChannelChangeCallback callback) {
        Preconditions.checkArgument(
                importer != null && !importer.trim().isEmpty(),
                "importer is null or empty"
                );
        callback = checkNotNull(callback, "callback is null");

        int [] stamp = new int[]{0};
        NavigableMap<String, ChannelChangeCallback> prev = null;
        ImmutableSortedMap.Builder<String,ChannelChangeCallback> mbldr = null;

        do {
            prev = m_callbacks.get(stamp);
            mbldr = ImmutableSortedMap.naturalOrder();
            mbldr.putAll(Maps.filterKeys(prev, not(equalTo(importer))));
            mbldr.put(importer, callback);
        } while (!m_callbacks.compareAndSet(prev, mbldr.build(), stamp[0], stamp[0]+1));
    }

    public void shutdown() {
        if (m_done.compareAndSet(false, true)) {
            m_es.shutdown();
            try {
                m_es.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                throw loggedDistributerException(e, "interrupted while waiting for executor termination");
            }
        }
    }

    @Override
    public void run() {
        if (m_done.get()) return;
        ChannelAssignment assignment = null;
        try {
            assignment = m_qref.get().poll(200, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw loggedDistributerException(e, "interrupted while polling for channel assignmanets");
        }
        if (assignment != null) {

            final SetMultimap<String, URI> added = mapByImporter(assignment.getAdded());
            final SetMultimap<String, URI> removed = mapByImporter(assignment.getRemoved());
            final SetMultimap<String, URI> assigned = mapByImporter(assignment.getChannels());

            NavigableMap<String, ChannelChangeCallback> callbacks = m_callbacks.getReference();
            for (Map.Entry<String, ChannelChangeCallback> e: callbacks.entrySet()) {
                final String importer = e.getKey();
                if (added.get(importer).isEmpty() && removed.get(importer).isEmpty()) {
                    continue;
                }
                final ChannelChangeCallback callback = e.getValue();
                final int version = assignment.getVersion();
                m_es.submit(new Runnable() {
                    @Override
                    public void run() {
                        if (m_done.get()) return;
                        try {
                            callback.onChange(
                                    added.get(importer),
                                    removed.get(importer),
                                    assigned.get(importer),
                                    version
                                    );
                        } catch (Exception e) {
                            throw loggedDistributerException(
                                    e, "failed to invoke channel changed callback for %s", importer
                                    );
                        }
                    }
                });
            }
            for (String noCallbackFor: Sets.difference(assigned.keySet(), callbacks.keySet())) {
                LOG.warn("Missing channel notification callbacks for importer \"" + noCallbackFor
                        + "\", which leave these channels \"" + assigned.get(noCallbackFor)
                        + "\" without any assigned handler"
                        );
            }
        }
        if (!m_done.get()) {
            m_es.submit(this);
        }
    }

    final static class CallbacksRef
        extends AtomicStampedReference<NavigableMap<String,ChannelChangeCallback>> {

        static final NavigableMap<String,ChannelChangeCallback> EMTPY_MAP =
                ImmutableSortedMap.of();

        public CallbacksRef(
                NavigableMap<String, ChannelChangeCallback> initialRef,
                int initialStamp) {
            super(initialRef, initialStamp);
        }

        public CallbacksRef() {
            this(EMTPY_MAP,0);
        }
    }
}
