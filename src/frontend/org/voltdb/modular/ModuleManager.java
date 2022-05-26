/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.modular;

import static com.google_voltpatches.common.base.Preconditions.checkNotNull;
import static com.google_voltpatches.common.base.Predicates.equalTo;
import static com.google_voltpatches.common.base.Predicates.not;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;
import org.voltcore.logging.VoltLogger;

import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.base.Joiner;
import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.collect.FluentIterable;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSortedMap;
import com.google_voltpatches.common.collect.Maps;

/**
 * Singleton Wrapper around OSGi module loading and unloading operations.
 */
public class ModuleManager {

    /*
     * Note for developers: please keep list in alpha-numerical order. Exclude ; and use only package names.
     */
    final static List<String> SYSTEM_PACKAGES = ImmutableList.<String>builder()
            .add("com.google_voltpatches.common.base;")
            .add("com.google_voltpatches.common.collect;")
            .add("com.google_voltpatches.common.io;")
            .add("com.google_voltpatches.common.net;")
            .add("com.google_voltpatches.common.util.concurrent;")
            .add("com.yammer.metrics;")
            .add("com.yammer.metrics.core;")
            .add("com.yammer.metrics.reporting;")
            .add("com.yammer.metrics.stats;")
            .add("com.yammer.metrics.util;")
            .add("jsr166y;")
            .add("org.apache.log4j;")
            .add("org.slf4j;")
            .add("org.voltcore.network;")
            .add("org.voltcore.logging;")
            .add("org.voltcore.utils;")
            .add("org.voltdb;include:=\"VoltType\",")
            .add("org.voltdb.client;")
            .add("org.voltdb.common;exclude=\"Permission\",")
            .add("org.voltdb.importer;")
            .add("org.voltdb.importer.formatter;")
            .add("org.voltdb.types;")
            .build();

    private static final VoltLogger LOG = new VoltLogger("HOST");

    private final static Joiner COMMA_JOINER = Joiner.on(",").skipNulls();

    private final static AtomicReference<File> CACHE_ROOT = new AtomicReference<>();

    private static ModuleManager m_self = null;

    public static void initializeCacheRoot(File cacheRoot) {
        if (CACHE_ROOT.compareAndSet(null, checkNotNull(cacheRoot))) {
            if (!cacheRoot.exists() && !cacheRoot.mkdirs()) {
                throw new SetUpException("Failed to create required OSGI cache directory: " + cacheRoot.getAbsolutePath());
            }
            if (   !cacheRoot.isDirectory()
                || !cacheRoot.canRead()
                || !cacheRoot.canWrite()
                || !cacheRoot.canExecute())
            {
                throw new SetUpException("Cannot access OSGI cache directory: " + cacheRoot.getAbsolutePath());
            }
            m_self = new ModuleManager(cacheRoot);
        }
    }

    public static void resetCacheRoot() {
        File cacheRoot = CACHE_ROOT.get();
        if (cacheRoot != null && CACHE_ROOT.compareAndSet(cacheRoot, null)) {
            try {
                m_self.m_framework.stop();
            } catch (BundleException bex) {
                //Ignore
            }
            m_self = null;
        }
    }

    private final static Function<String,String> appendVersion = new Function<String, String>() {
        @Override
        public String apply(String input) {
            return input + "version=1.0.0";
        }
    };

    public static ModuleManager instance() {
        return m_self;
    }

    static ModularException loggedModularException(Throwable e, String msg, Object...args) {
        ModularException.isCauseFor(e).map(me -> { throw me; });
        LOG.error(String.format(msg, args), e);
        return new ModularException(msg, e, args);
    }

    public static URI bundleURI(File fl) {
        return fl.toPath().toUri();
    }

    private final Framework m_framework;
    private final BundleRef m_bundles;

    private ModuleManager(File cacheRoot) {

        String systemPackagesSpec = FluentIterable
                .from(SYSTEM_PACKAGES)
                .transform(appendVersion)
                .join(COMMA_JOINER);

        Map<String, String> frameworkProps  = ImmutableMap.<String,String>builder()
                .put(Constants.FRAMEWORK_SYSTEMPACKAGES_EXTRA, systemPackagesSpec)
                .put("org.osgi.framework.storage.clean", "onFirstInit")
                .put("felix.cache.rootdir", cacheRoot.getAbsolutePath())
                .put("felix.cache.locking", Boolean.FALSE.toString())
                .build();

        LOG.info("Framework properties are: " + frameworkProps);

        FrameworkFactory frameworkFactory = ServiceLoader
                .load(FrameworkFactory.class)
                .iterator()
                .next();

        m_framework = frameworkFactory.newFramework(frameworkProps);

        try {
            m_framework.start();
        } catch (BundleException e) {
            LOG.error("Failed to start the felix OSGi framework", e);
            throw new SetUpException("Failed to start the felix OSGi framework", e);
        }

        m_bundles = new BundleRef(m_framework);
    }

    /**
     * Gets the service from the given bundle jar uri. Loads and starts the bundle
     * if it isn't yet loaded
     *
     * @param bundleURI bundle jar URI
     * @param svcClazz the service class exposed by the bundle jar
     * @return a reference to an instance of the service class
     */
    public <T> T getService(URI bundleURI, Class<T> svcClazz) {
        return m_bundles.getService(bundleURI, svcClazz);
    }

    public void unload(URI bundleURI) {
        m_bundles.stopBundle(bundleURI);
    }

    public void unload(Set<URI> uris) {
        m_bundles.stopBundles(uris);
    }

    public void uninstall(URI bundleURI) {
        m_bundles.uninstallBundle(bundleURI);
    }

    public void uninstall(Set<URI> uris) {
        m_bundles.uninstallBundles(uris);
    }

    public static class SetUpException extends RuntimeException {

        private static final long serialVersionUID = 8197183357774453653L;

        public SetUpException() {
        }

        public SetUpException(String message, Throwable cause) {
            super(message, cause);
        }

        public SetUpException(String message) {
            super(message);
        }

        public SetUpException(Throwable cause) {
            super(cause);
        }
    }

    static class BundleRef extends AtomicReference<NavigableMap<URI,Bundle>> {

        private static final long serialVersionUID = -3691039780541403034L;
        static NavigableMap<URI,Bundle> EMPTY_MAP = ImmutableSortedMap.of();

        final Framework m_framework;

        public BundleRef(Framework framework, NavigableMap<URI,Bundle> initialRef) {
            super(initialRef);
            m_framework = framework;
        }

        public BundleRef(Framework framework) {
            this(framework, EMPTY_MAP);
        }

        private Bundle startBundle(URI bundleURI) {
            NavigableMap<URI,Bundle> expect, update;
            Bundle bundle = null;
            do {
                expect = get();
                if (expect.containsKey(bundleURI)) break;

                BundleContext ctx = m_framework.getBundleContext();
                bundle = ctx.getBundle(bundleURI.toASCIIString());
                if (bundle != null) {
                    try {
                        bundle.update();
                    } catch (BundleException e) {
                        String msg = e.getMessage();
                        throw loggedModularException(e, "Unable to update bundle %s. %s", bundleURI, msg);
                    } catch (Throwable t) {
                        throw loggedModularException(t, "Unable to update bundle %s", bundleURI);
                    }
                } else {
                    try {
                        bundle = ctx.installBundle(bundleURI.toASCIIString());
                    } catch (BundleException e) {
                        String msg = e.getMessage();
                        throw loggedModularException(e, "Unable to install bundle %s. %s", bundleURI, msg);
                    } catch (Throwable t) {
                        throw loggedModularException(t, "Unable to instal bundle %s", bundleURI);
                    }
                }
                try {
                    bundle.start();
                } catch (BundleException e) {
                    String msg = e.getMessage();
                    throw loggedModularException(e, "Unable to start bundle %s. %s", bundleURI, msg);
                } catch (Throwable t) {
                    throw loggedModularException(t, "Unable to start bundle %s", bundleURI);
                }

                update = ImmutableSortedMap.<URI,Bundle>naturalOrder()
                        .putAll(expect)
                        .put(bundleURI, bundle)
                        .build();
            } while (!compareAndSet(expect, update));

            return get().get(bundleURI);
        }

        <T> T getService(URI bundleURI, Class<T> svcClazz) {
            Bundle bundle = get().get(bundleURI);
            if (bundle == null) {
                synchronized(this) {
                    bundle = startBundle(bundleURI);
                }
            }
            BundleContext ctx = bundle.getBundleContext();
            for (ServiceReference<?> ref: bundle.getRegisteredServices()) {
                if (ref.isAssignableTo(bundle, svcClazz.getName())) {
                    return svcClazz.cast(ctx.getService(ref));
                }
            }
            return null;
        }

        Optional<Bundle> stopBundle(URI bundleURI) {
            NavigableMap<URI,Bundle> expect, update;
            do {
                expect = get();
                update = ImmutableSortedMap.<URI,Bundle>naturalOrder()
                        .putAll(Maps.filterKeys(expect, not(equalTo(bundleURI))))
                        .build();
            } while (expect.containsKey(bundleURI) && !compareAndSet(expect, update));

            Bundle bundle = expect.get(bundleURI);
            if (bundle != null) {
                try {
                    bundle.stop();
                } catch (BundleException e) {
                    throw loggedModularException(e, "Failed to stop bundle %s", bundleURI);
                }
            }
            return Optional.ofNullable(bundle);
        }

        void uninstallBundle(URI bundleURI) {
            stopBundle(bundleURI).ifPresent( (Bundle b) -> {
                try {
                    b.uninstall();
                } catch (Throwable t) {
                    throw loggedModularException(t, "Failed to uninstall %s", b.getLocation());
                }
            });
        }

        NavigableMap<URI, Bundle> stopBundles(Set<URI> bundles) {
            NavigableMap<URI,Bundle> expect, update;
            do {
                expect = get();
                update = ImmutableSortedMap.<URI,Bundle>naturalOrder()
                        .putAll(Maps.filterKeys(expect, not(in(bundles))))
                        .build();
            } while (!compareAndSet(expect, update));

            List<URI> couldNotStop = new ArrayList<>();
            NavigableMap<URI,Bundle> stopped = Maps.filterKeys(expect,in(bundles));
            for (Map.Entry<URI,Bundle> e: stopped.entrySet()) {
                URI bundleURI = e.getKey();
                Bundle bundle = e.getValue();
                try {
                    bundle.stop();
                } catch (BundleException exc) {
                    LOG.error("Failed to stop bundle " + bundleURI, exc);
                    couldNotStop.add(bundleURI);
                }
            }
            if (!couldNotStop.isEmpty()) {
                throw new ModularException("Failed to stop bundles %s", couldNotStop);
            }
            return stopped;
        }

        void uninstallBundles(Set<URI> bundles) {
            List<URI> couldNotUninstall = new ArrayList<>();
            for (Map.Entry<URI,Bundle> e: stopBundles(bundles).entrySet()) {
                URI bundleURI = e.getKey();
                Bundle bundle = e.getValue();
                try {
                    bundle.uninstall();
                } catch (BundleException exc) {
                    LOG.error("Failed to uninstall bundle " + bundleURI, exc);
                    couldNotUninstall.add(bundleURI);
                }
                if (!couldNotUninstall.isEmpty()) {
                    throw new ModularException("Failed to uninstall bundles %s", couldNotUninstall);
                }
            }
        }
    }

    public final static <T> Predicate<T> in(final Set<T> set) {
        return new Predicate<T>() {
            @Override
            public boolean apply(T m) {
                return set.contains(m);
            }
        };
    }
}
