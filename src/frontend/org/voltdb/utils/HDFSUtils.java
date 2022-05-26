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
package org.voltdb.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.ProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;

import com.google_voltpatches.common.collect.ImmutableList;

public class HDFSUtils {

    private final static Pattern OPRE = Pattern.compile("&?\\bop(?:=(?:\\w*)){0,1}");
    private final static Pattern PERCENT = Pattern.compile("%");

    final static Pattern HDFSPATH = Pattern.compile(
            "\\A/webhdfs/v1(?<path>/.+\\z)",
            Pattern.CASE_INSENSITIVE
            );

    private final static List<NameValuePair> overwrite =
            ImmutableList.<NameValuePair>of(new BasicNameValuePair("overwrite", "true"));

    private final static List<NameValuePair> dontoverwrite =
            ImmutableList.<NameValuePair>of(new BasicNameValuePair("overwrite", "false"));

    public static final Header OctetStreamContentTypeHeader =
            new BasicHeader("Content-Type", ContentType.APPLICATION_OCTET_STREAM.getMimeType());

    public static boolean containsOpQuery(URI url) {
        final String qry = url.getQuery();
        return qry != null && OPRE.matcher(qry).find();
    }

    public static URI opAdder(URI url, String op, List<NameValuePair> queryPairs) {
        StringBuilder query = new StringBuilder(128).append("op=").append(op);
        for (NameValuePair nvp: queryPairs) {
            query.append("&").append(nvp.getName());
            if (nvp.getValue() != null) {
                query.append("=").append(nvp.getValue());
            }
        }
        if (url.getQuery() != null && !url.getQuery().trim().isEmpty()) {
            if (OPRE.matcher(url.getQuery()).find()) {
                throw new IllegalArgumentException("Invalid URI: Query cannot contain op field");
            }
            query.append("&").append(url.getQuery());
        }
        try {
            url = new URI(url.getScheme(), url.getAuthority(), url.getPath(), query.toString(), url.getFragment());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URI " + url, e);
        }
        return url;
    }

    public static URI opAdder(URI url, String op, String...params) {
        if ((params.length % 2) != 0) {
            throw new IllegalArgumentException(
                    "odd number of parameters given, when expecting a list of name value pairs"
                    );
        }
        ImmutableList.Builder<NameValuePair> nvs = ImmutableList.builder();
        for (int i = 0; i < params.length; i+=2) {
            nvs.add(new BasicNameValuePair(params[i], params[i+1]));
        }
        return opAdder(url, op, nvs.build());
    }

    public static URI opAdder(URI url, String op) {
        return opAdder(url, op, Collections.<NameValuePair>emptyList());
    }

    public static HttpPut createFileRequest(URI url) {
        return new HttpPut(opAdder(url, "CREATE", dontoverwrite));
    }

    public static HttpPut createOrOverwriteFileRequest(URI url) {
        return new HttpPut(opAdder(url, "CREATE", overwrite));
    }

    public static HttpGet createFileStatusRequest(URI url) {
        return new HttpGet(opAdder(url, "GETFILESTATUS"));
    }

    public static HttpPut createRenameRequest(URI url, String to) {
        return new HttpPut(opAdder(url,"RENAME","destination",to));
    }

    public static HttpPut createSetReplicationRequest(URI url, int replicationSize) {
        return new HttpPut(opAdder(url,"SETREPLICATION", "replication", Integer.toString(replicationSize)));
    }
    // the webhdfs http request that this method creates will create all directories along the path that don't already exist
    public static HttpPut createDirectoryRequest(URI url) {
        return new HttpPut(opAdder(url, "MKDIRS"));
    }

    public static String getHdfsPath(URI uri) {
        if (uri == null) {
            throw new IllegalArgumentException("null uri parameter");
        }
        Matcher mtc = HDFSPATH.matcher(uri.getPath());
        if (!mtc.matches()) {
            throw new IllegalArgumentException("path \"" + uri + "\" is not a webdfs URL");
        }
        return mtc.group("path");
    }

    /**
     * Convenient method to check if the given URI string is a WebHDFS URL.
     * See {@link #isHdfsUri(java.net.URI)} for more.
     */
    public static boolean isHdfsUri(String uriStr)
    {
        return isHdfsUri(URI.create(PERCENT.matcher(uriStr).replaceAll("")));
    }

    /**
     * Checks if the given URI is a WebHDFS URL. WebHDFS URLs have the form
     * http[s]://hostname:port/webhdfs/v1/.
     *
     * @param endpoint    The URI
     * @return true if it is a WebHDFS URL, false otherwise.
     */
    public static boolean isHdfsUri(URI endpoint)
    {
        final String path = endpoint.getPath();
        if (path != null && path.indexOf('/', 1) != -1) {
            return path.substring(1, path.indexOf('/', 1)).equalsIgnoreCase("webhdfs");
        } else {
            return false;
        }
    }

    public static class HadoopRedirectStrategy extends DefaultRedirectStrategy {
        @Override
        public boolean isRedirected(HttpRequest request, HttpResponse response, HttpContext context)  {
            boolean isRedirect=false;
            try {
                isRedirect = super.isRedirected(request, response, context);
            } catch (ProtocolException e) {
                e.printStackTrace();
            }
            if (!isRedirect) {
                int responseCode = response.getStatusLine().getStatusCode();
                if (responseCode == 301 || responseCode == 302 || responseCode == 307) {
                    isRedirect = true;
                }
            }
            return isRedirect;
        }

        @Override
        public HttpUriRequest getRedirect(HttpRequest request, HttpResponse response, HttpContext context) throws ProtocolException {
            URI uri = getLocationURI(request, response, context);
            HttpUriRequest redirectRequest = null;
            String method = request.getRequestLine().getMethod();
            if (method.equalsIgnoreCase(HttpHead.METHOD_NAME)) {
                redirectRequest = new HttpHead(uri);
            } else if (method.equalsIgnoreCase(HttpPost.METHOD_NAME)) {
                HttpPost post = new HttpPost(uri);
                post.setEntity(((HttpEntityEnclosingRequest) request).getEntity());
                redirectRequest = post;
                if (isHdfsUri(uri)) {
                    redirectRequest.setHeader("Expect", "100-continue");
                }
                if (post.getEntity() == null || post.getEntity().getContentLength() == 0) {
                    post.setHeader(OctetStreamContentTypeHeader);
                }
            } else if (method.equalsIgnoreCase(HttpGet.METHOD_NAME)) {
                redirectRequest = new HttpGet(uri);
            } else if (method.equalsIgnoreCase(HttpPut.METHOD_NAME)) {
                HttpPut put = new HttpPut(uri);
                put.setEntity(((HttpEntityEnclosingRequest) request).getEntity());
                redirectRequest = put;
                if (isHdfsUri(uri)) {
                    redirectRequest.setHeader("Expect", "100-continue");
                }
                if (put.getEntity() == null || put.getEntity().getContentLength() == 0) {
                    put.setHeader(OctetStreamContentTypeHeader);
                }
            }
            return redirectRequest;
        }

    }
}
