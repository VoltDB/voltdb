/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.exportclient.decode;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.voltdb.exportclient.ExportDecoderBase.BinaryEncoding;

import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.collect.FluentIterable;
import com.google_voltpatches.common.collect.ImmutableList;

public class NVPairsDecoder extends RowDecoder<List<NameValuePair>, RuntimeException> {

    protected final StringArrayDecoder m_stringArrayDecoder;
    protected final String [] m_columnNames;

    protected NVPairsDecoder(StringArrayDecoder stringArrayDecoder) {
        super(stringArrayDecoder);
        m_stringArrayDecoder = stringArrayDecoder;
        m_columnNames = ImmutableList.copyOf(m_typeMap.keySet()).toArray(new String[0]);
    }

    @Override
    public List<NameValuePair> decode(List<NameValuePair> to, Object[] fields)
            throws RuntimeException {
        ImmutableList.Builder<NameValuePair> lb = null;
        if (to == null) {
            lb = ImmutableList.builder();
        }
        String [] values = m_stringArrayDecoder.decode(null, fields);
        for (int i = 0; i < values.length && i < m_columnNames.length; ++i) {
            NameValuePair pair = new BasicNameValuePair(m_columnNames[i], percentEncode(values[i]));
            if (lb != null) {
                lb.add(pair);
            } else {
                to.add(pair);
            }
        }
        return lb != null ? lb.build() : to;
    }

    /**
     * Encode the given string using percent encoding in UTF-8 so that it's safe
     * to be included in the URL.
     * @param s    The string to encode
     * @return Encoded string.
     */
    public static String percentEncode(String s) {
        try {
            if (s == null) {
                return "%00"; // URL encode null - rfc3986 & https://www.w3schools.com/tags/ref_urlencode.asp
            }
            return URLEncoder.encode(s, "UTF-8")
                    .replace("+", "%20")
                    .replace("*", "%2A")
                    .replace("%7E", "~");
        } catch (UnsupportedEncodingException e) {
            // should never happen
            return null;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends RowDecoder.Builder {
        protected final StringArrayDecoder.Builder m_delegateBuilder = new StringArrayDecoder.Builder();

        protected final static Function<String, String> percentEncodeName = new Function<String, String>() {
            @Override
            public String apply(String input)
            {
                return percentEncode(input);
            }
        };

        public Builder() {
            m_delegateBuilder.nullRepresentation(null);
            m_delegateBuilder.dateFormatter("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            m_delegateBuilder.binaryEncoding(BinaryEncoding.BASE64);
        }

        public Builder timeZone(String timeZoneID) {
            m_delegateBuilder.timeZone(timeZoneID);
            return this;
        }

        public NVPairsDecoder build() {
            columnNames(FluentIterable.from(m_columnNames)
                    .transform(camelCaseNameUpperFirst)
                    .transform(percentEncodeName)
                    .toList());
            m_delegateBuilder.use(this);
            return new NVPairsDecoder(m_delegateBuilder.build());
        }
    }
}
