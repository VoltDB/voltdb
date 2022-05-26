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

package org.voltdb.exportclient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.util.EntityUtils;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.ExportManager;
import org.voltdb.export.ExportManagerInterface;
import org.voltdb.export.ExportManagerInterface.ExportMode;
import org.voltdb.exportclient.decode.BatchDecoder.BulkException;
import org.voltdb.exportclient.decode.ElasticSearchJsonEntityDecoder;
import org.voltdb.exportclient.decode.EndpointExpander;
import org.voltdb.exportclient.decode.EntityDecoder;
import org.voltdb.exportclient.decode.JsonStringDecoder;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Lists;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

public class ElasticSearchHttpExportClient extends ExportClientBase
{
    private static final ExportClientLogger LOG = new ExportClientLogger();

    // Max number of pooled HTTP connections to the endpoint
    private static final int HTTP_EXPORT_MAX_CONNS = Integer.getInteger(
            "HTTP_EXPORT_MAX_CONNS", 20);

    static enum HttpMethod {POST} // PUT method not support Automatic ID Generation

    static enum DecodeType {
        JSONString(ContentType.APPLICATION_JSON.withCharset(StandardCharsets.UTF_8)), // for single index api
        JSONEntity(ElasticSearchJsonEntityDecoder.JsonContentType); // for bulk api

        private final ContentType m_contentType;

        DecodeType(ContentType contentType) {
            m_contentType = contentType;
        }

        public ContentType contentType() {
            return m_contentType;
        }
    }

    static final EnumSet<DecodeType> BatchDecodeTypes = EnumSet.of(DecodeType.JSONEntity);

    // static LoginContext m_context;

    enum DecodedStatus {

        OK(null),
        FAIL(null),
        BULK_OPERATION_FAILED("BulkOperationFailed");

        static final Map<String, DecodedStatus> exceptions;

        static
        {
            ImmutableMap.Builder<String, DecodedStatus> builder = ImmutableMap
                    .builder();
            for (DecodedStatus drsp : values())
            {
                if (drsp.exception != null)
                {
                    builder.put(drsp.exception, drsp);
                }
            }
            exceptions = builder.build();
        }

        String exception;

        DecodedStatus(String exception) {
            this.exception = exception;
        }

        static DecodedStatus fromResponse(HttpResponse rsp) {

            if (rsp == null) {
                return FAIL;
            }
            String msg = "";
            JSONObject json = new JSONObject();
            if (rsp.getEntity().getContentLength() > 0) {
                try {
                    msg = EntityUtils.toString(rsp.getEntity(), StandardCharsets.UTF_8);
                    LOG.trace("Notification response: ", msg);
                }
                catch (ParseException | IOException e) {
                    LOG.warn("could not trace response body",e);
                }
            }
            if (msg != null && !msg.trim().isEmpty()) {
                try {
                    json = new JSONObject(msg);
                }
                catch (JSONException e) {
                    LOG.warn("could not load response body to parse error message",e);
                }
            }

            switch (rsp.getStatusLine().getStatusCode()) {
            case HttpStatus.SC_OK:
            case HttpStatus.SC_CREATED:
            case HttpStatus.SC_ACCEPTED:
                // for handling bulk
                if (json.optBoolean("errors")) {
                    return BULK_OPERATION_FAILED;
                }
                return OK;

            case HttpStatus.SC_BAD_REQUEST:
            case HttpStatus.SC_NOT_FOUND:
            case HttpStatus.SC_FORBIDDEN:
                return FAIL;

            default:
                return FAIL;
            }
        }
    }

    String m_endpoint = null;
    TimeZone m_timeZone = VoltDB.REAL_DEFAULT_TIMEZONE;
    ContentType m_contentType = ContentType.APPLICATION_JSON;
    DecodeType m_decodeType = DecodeType.JSONEntity;
    boolean m_batchMode = true;
    boolean m_isKrb;

    private CloseableHttpAsyncClient m_client = HttpAsyncClients.createDefault();
    private PoolingNHttpClientConnectionManager m_connManager = null;

    @Override
    public void configure(Properties config) throws Exception
    {
        m_endpoint = config.getProperty("endpoint","").trim();
        if (m_endpoint.isEmpty()) {
            throw new IllegalArgumentException("HttpExportClient: must provide an endpoint");
        }
        try {
            URI uri = new URI(EndpointExpander.expand(m_endpoint, "CONFIGURATION_CHECK", 0, 123L, new Date()));
            final String scheme = uri.getScheme() != null ? uri.getScheme().toLowerCase() : null;
            if (!("http".equals(scheme) || "https".equals(scheme))) {
                throw new IllegalArgumentException("only 'http' or 'https' endpoints are supported");
            }
        }
        catch (URISyntaxException | IllegalArgumentException e) {
            throw new IllegalArgumentException("could not expand endpoint " + m_endpoint, e);
        }

        String timeZoneID = config.getProperty("timezone", "").trim();
        if (!timeZoneID.isEmpty()) {
            m_timeZone = TimeZone.getTimeZone(timeZoneID);
        }

        m_batchMode = Boolean.parseBoolean(config.getProperty("batch.mode","true"));

        // just for elastic search
        if (m_batchMode) {
            m_decodeType = DecodeType.JSONEntity;
        }
        else {
            m_decodeType = DecodeType.JSONString;
        }

        m_contentType = m_decodeType.contentType();


        LOG.debug("Starting Elastic Export client with %s", m_endpoint);

        //Dont do actual config in check mode.
        boolean configcheck = Boolean.parseBoolean(config.getProperty(ExportManager.CONFIG_CHECK_ONLY, "false"));
        if (configcheck) {
            return;
        }

        // not support right now
        m_isKrb = false;

        connect();
    }

    /**
     * Construct an async HTTP client with connection pool.
     * @throws IOReactorException
     */
    private void connect() throws IOReactorException
    {
        if (m_connManager == null) {
            ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
            m_connManager = new PoolingNHttpClientConnectionManager(ioReactor);
            m_connManager.setMaxTotal(HTTP_EXPORT_MAX_CONNS);
            m_connManager.setDefaultMaxPerRoute(HTTP_EXPORT_MAX_CONNS);
        }

        if (m_client == null || !m_client.isRunning()) {
            HttpAsyncClientBuilder client = HttpAsyncClients.custom().setConnectionManager(m_connManager);

            if (m_isKrb) {
                // m_client = (CloseableHttpAsyncClient)Subject.doAs(m_context.getSubject(), new PrivilegedBuild(client));
            }
            else {
                m_client = client.build();
            }
            m_client.start();
        }
    }

    @Override
    public void shutdown(){
        try {
            m_client.close();
            m_connManager.shutdown(60 * 1000);
        } catch (IOException e) {
            LOG.error("Error closing the HTTP client", e);
        }
    }

    /**
     * Generate the HTTP request.
     * @param uri            The request URI
     * @param requestBody    The request body, URL encoded if necessary
     * @return The HTTP request.
     */
    private HttpUriRequest makeRequest(URI uri, final String requestBody)
    {
        HttpPost post = new HttpPost(uri);
        post.setEntity(new StringEntity(requestBody, m_contentType));
        return post;
    }

    private HttpUriRequest makeBatchRequest(URI uri, final AbstractHttpEntity entity) {
        // ElasticSearch only accepts POST requests with application/json content type
        if (entity != null) {
            entity.setContentType(ContentType.APPLICATION_JSON.getMimeType());
        }
        HttpPost post = new HttpPost(uri);
        post.setEntity(entity);
        return post;
    }


    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new HttpExportDecoder(source);
    }

    class HttpExportDecoder extends ExportDecoderBase {
        private final ListeningExecutorService m_es;
        private final EntityDecoder m_entityDecoder;
        private final JsonStringDecoder m_jsonStringDecoder;
        private final List<Future<HttpResponse>> m_outstanding = Lists.newArrayList();
        private URI m_exportPath = null;

        // exposing it for test purpose
        URI getExportPath() { return m_exportPath; }

        @Override
        public ListeningExecutorService getExecutor() {
            return m_es;
        }

        public HttpExportDecoder(AdvertisedDataSource source) {
            super(source);

            if (m_batchMode) {
                EntityDecoder entityDecoder = null;
                ElasticSearchJsonEntityDecoder.Builder entityBuilder = ElasticSearchJsonEntityDecoder.builder();
                entityBuilder
                    .timeZone(m_timeZone)
                    .skipInternalFields(true)
                ;
                entityDecoder = entityBuilder.build();
                m_entityDecoder = entityDecoder;
                m_jsonStringDecoder = null;

            }
            else {
                JsonStringDecoder.Builder builder = JsonStringDecoder.builder();
                builder
                    .timeZone(m_timeZone)
                    .skipInternalFields(true)
                ;
                m_jsonStringDecoder = builder.build();
                m_entityDecoder = null;
            }
            if (VoltDB.getExportManager().getExportMode() == ExportMode.BASIC) {
                // TODO: how to make it named unique
                m_es = CoreUtils.getListeningSingleThreadExecutor(
                        "Elastic Export Decoder for partition " + source.partitionId, CoreUtils.MEDIUM_STACK_SIZE);
            } else {
                m_es = null;
            }
        }

        void populateExportPath(String tableName, int partitionId, long generation) {
            String endpoint = EndpointExpander.expand(
                    m_endpoint,
                    tableName.toLowerCase(), // elastic search index should be lower case
                    partitionId,
                    generation,
                    new Date(),
                    m_timeZone
                    );
            URI path = null;
            // just for elastic search
            if (m_batchMode)  {
                endpoint = new StringBuilder(endpoint.length()+6).append(endpoint).append("/_bulk").toString();
            }

            try {
                path = new URI(endpoint);
            }
            catch (URISyntaxException e) {
                // should not get here as the endpoint URL syntax was validated at configure
                LOG.error("Unable to create URI %s ", e, endpoint);
                Throwables.propagate(e);
            }
            m_exportPath = path;
        }

        @Override
        public boolean processRow(ExportRow row) throws RestartBlockException {
            URI exportPath = m_exportPath;
            if (m_client == null || !m_client.isRunning()) {
                try {
                    connect();
                }
                catch (IOReactorException e) {
                    LOG.error("Unable to create HTTP client", e);
                    throw new RestartBlockException("Unable to create HTTP client",e,true);
                }
            }

            if (m_batchMode) {
                try {
                    m_entityDecoder.add(row.generation, row.tableName, row.types, row.names, row.values);
                    return true;
                }
                catch (BulkException e) {
                    // non restartable structural failure
                    LOG.error("unable to acummulate export records in batch mode", e);
                    return false;
                }
            }

            HttpUriRequest request = null;
            try {
                request = makeRequest(exportPath, m_jsonStringDecoder.decode(row.generation, row.tableName, row.types, row.names, null, row.values));
            }
            catch (JSONException e) {
                // non restartable structural failure
                LOG.error("unable to build an HTTP request from an exported row", e);
                return false;
            }

            try {
                m_outstanding.add(m_client.execute(request, null));
            }
            catch (Exception e) {
                // May be recoverable, retry with a backoff
                LOG.error("Unable to dispatch a request to \"%s\"", e, request);
                throw new RestartBlockException("Unable to dispatch a request to \"" + request + "\".", e, true);
            }

            return true;
        }

        @Override
        public void onBlockStart(ExportRow row) throws RestartBlockException {
            m_outstanding.clear();
            if (m_exportPath == null) {
                populateExportPath(row.tableName,  row.partitionId, row.generation);
            }
        }

        @Override
        public void onBlockCompletion(ExportRow row) throws RestartBlockException {
            final URI exportPath = m_exportPath;
            if (m_batchMode) {
                HttpUriRequest rqst = null;
                try {
                    rqst = makeBatchRequest(
                            exportPath, m_entityDecoder.harvest(row.generation)
                            );
                    Future<HttpResponse> fut = m_client.execute(rqst, null);

                    // error handling
                    DecodedStatus status = checkResponse(fut.get());
                    if (status != DecodedStatus.OK) {
                        throw new RestartBlockException("requeing on failed response check: " + status, true);
                    }
                } catch (Exception e) {
                    // May be recoverable, retry with a backoff
                    LOG.error("Unable to complete request to \"%s\"",e,rqst);
                    throw new RestartBlockException("Unable to complete request to \"" + rqst + "\".",e,true);
                }
            }

            for (Future<HttpResponse> request : m_outstanding) {
                try {
                    if (checkResponse(request.get()) != DecodedStatus.OK) {
                        throw new RestartBlockException("requeing on failed response check", true);
                    }
                } catch (Exception e) {
                    LOG.error("Failure reported in request response.", e);
                    throw new RestartBlockException("Failure reported in request response.", true);
                }
            }
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            if (m_entityDecoder != null) {
                m_entityDecoder.discard(0L);
            }
            if (m_es != null) {
                m_es.shutdown();
                try {
                    m_es.awaitTermination(365, TimeUnit.DAYS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        DecodedStatus checkResponse(HttpResponse response)
        {
            DecodedStatus status = DecodedStatus.fromResponse(response);
            if (status == DecodedStatus.FAIL || status == DecodedStatus.BULK_OPERATION_FAILED ) {
                LOG.error("Notification request failed with %s", response.getStatusLine().toString());
            }
            return status;
        }
    }
}
