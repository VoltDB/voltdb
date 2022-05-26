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

package org.voltdb.importer;

import java.net.URI;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltcore.utils.Punycode;

import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.base.Joiner;
import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Predicate;

public class ChannelSpec implements Comparable<ChannelSpec> {

    static final Pattern specRE = Pattern
            .compile("(?<importer>(?:[\\w_-]+\\.)*[\\w_-]+)\\|(?<encodeduri>(?:[\\w-_\\.]+))");
    static Joiner nodeJoiner = Joiner.on('/').skipNulls();

    final String importer;
    final URI uri;

    public ChannelSpec(String importer, URI uri) {
        this.uri = Preconditions.checkNotNull(uri, "channel URI is null");
        Preconditions.checkArgument(
                importer != null && !importer.trim().isEmpty(),
                "importer is null or empty"
                );
        this.importer = importer;
    }

    public ChannelSpec(String importer, String uri) {
        this(importer, URI.create(uri));
    }

    public ChannelSpec(String jsonValue) {
        Preconditions.checkArgument(
                jsonValue != null && !jsonValue.trim().isEmpty(),
                "node is null or empty"
                );
        Matcher mtc = specRE.matcher(jsonValue);
        Preconditions.checkArgument(
                mtc.matches(),
                "Invalid encoded JSON value specification for %s",
                jsonValue
                );
        String strUri = Punycode.decode(mtc.group("encodeduri"));
        this.uri = URI.create(strUri);
        this.importer = mtc.group("importer");
    }

    public String asJSONValue() {
        StringBuilder sb = new StringBuilder(128);
        sb.append(importer).append('|');
        sb.append(Punycode.encode(uri.toString()));
        return sb.toString();
    }

    public String getImporter() {
        return importer;
    }

    public URI getUri() {
        return uri;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((importer == null) ? 0 : importer.hashCode());
        result = prime * result + ((uri == null) ? 0 : uri.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ChannelSpec other = (ChannelSpec) obj;
        if (importer == null) {
            if (other.importer != null)
                return false;
        } else if (!importer.equals(other.importer))
            return false;
        if (uri == null) {
            if (other.uri != null)
                return false;
        } else if (!uri.equals(other.uri))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ChannelSpec [importer=" + importer + ", uri=" + uri + "]";
    }

    @Override
    public int compareTo(ChannelSpec o) {
        int cmp = importer.compareTo(o.importer);
        if (cmp == 0) {
            cmp = uri.compareTo(o.uri);
        }
        return cmp;
    }

    public final static Predicate<ChannelSpec> importerIs(final String importer) {
        return new Predicate<ChannelSpec>() {
            @Override
            final public boolean apply(ChannelSpec spec) {
                return importer.equals(spec.importer);
            }
        };
    }

    public static <V> Predicate<Map.Entry<ChannelSpec, V>> importerKeyIs(final String importer, Class<V> clazz) {
        return new Predicate<Map.Entry<ChannelSpec, V>> () {
            @Override
            public boolean apply(Entry<ChannelSpec, V> e) {
                return importer.equals(e.getKey().getImporter());
            }
        };
    }

    public static <V> Predicate<Map.Entry<ChannelSpec, V>> specKeyIs(final ChannelSpec spec, Class<V> clazz) {
        return new Predicate<Map.Entry<ChannelSpec, V>> () {
            @Override
            public boolean apply(Entry<ChannelSpec, V> e) {
                return spec.equals(e.getKey());
            }
        };
    }

    public static <V> Predicate<Map.Entry<ChannelSpec, V>> specKeyIn(final Set<ChannelSpec> specs, Class<V> clazz) {
        return new Predicate<Map.Entry<ChannelSpec, V>> () {
            @Override
            public boolean apply(Entry<ChannelSpec, V> e) {
                return specs.contains(e.getKey());
            }
            @Override
            public String toString() {
                return "Predicate.specKeyIn [Map.Entry.getKey() is contained in " + specs + " ]";
            }
         };
    }

    public static <K> Predicate<Map.Entry<K, ChannelSpec>> importerValueIs(final String importer, Class<K> clazz) {
        return new Predicate<Map.Entry<K, ChannelSpec>> () {
            @Override
            public boolean apply(Entry<K, ChannelSpec> e) {
                return importer.equals(e.getValue().getImporter());
            }
        };
    }

    public static <K> Predicate<Map.Entry<K, ChannelSpec>> specValueIs(final ChannelSpec spec, Class<K> clazz) {
        return new Predicate<Map.Entry<K, ChannelSpec>> () {
            @Override
            public boolean apply(Entry<K, ChannelSpec> input) {
                return spec.equals(input.getValue());
            }
        };
    }

    public final static Function<String, ChannelSpec> asSelf = new Function<String, ChannelSpec>() {
        @Override
        final public ChannelSpec apply(String input) {
            return new ChannelSpec(input);
        }
    };

    public final static Function<URI,ChannelSpec> fromUri(final String importer) {
        return new Function<URI,ChannelSpec>() {
            @Override
            public ChannelSpec apply(URI uri) {
                return new ChannelSpec(importer,uri);
            }
        };
    }
}
