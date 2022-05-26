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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.util.EntityUtils;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.exportclient.decode.AvroEntityDecoder;
import org.voltdb.exportclient.decode.CSVEntityDecoder;
import org.voltdb.exportclient.decode.EndpointExpander;
import org.voltdb.exportclient.decode.EntityDecoder;
import org.voltdb.exportclient.decode.NVPairsDecoder;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.HDFSUtils;
import org.voltdb.utils.TimeUtils;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Lists;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import java.util.Objects;

import org.voltdb.export.ExportManager;
import org.voltdb.export.ExportManagerInterface.ExportMode;

import static org.voltdb.utils.HDFSUtils.OctetStreamContentTypeHeader;

public class HttpExportClient extends ExportClientBase {
    private static final VoltLogger m_logger = new VoltLogger("ExportClient");
    private static final int LOG_RATE_LIMIT = 10; // seconds

    private static final Pattern uriRE = Pattern.compile("\\A([\\w-]+)://");
    private static final Pattern modtimeRE = Pattern.compile("\"modificationTime\":(?<modtime>\\d+)");

    // Max number of pooled HTTP connections to the endpoint
    private static final int HTTP_EXPORT_MAX_CONNS = Integer.getInteger("HTTP_EXPORT_MAX_CONNS", 20);

    static final String HmacSHA1 = "HmacSHA1";
    static final String HmacSHA256 = "HmacSHA256";

    static enum HttpMethod { GET, POST, PUT }
    static enum DecodeType {
        FORM(ContentType.APPLICATION_FORM_URLENCODED),
        CSV(CSVEntityDecoder.CSVContentType),
        AVRO(AvroEntityDecoder.AvroContentType);

        private final ContentType m_contentType;

        DecodeType(ContentType contentType) {
            m_contentType = contentType;
        }

        public ContentType contentType() {
            return m_contentType;
        }
    }

    static final String OctetStreamMimeType =
            ContentType.APPLICATION_OCTET_STREAM.getMimeType();

    static final EnumSet<DecodeType> BatchDecodeTypes =
            EnumSet.of(DecodeType.CSV,DecodeType.AVRO);

    static LoginContext m_context;

    enum DecodedStatus {

        OK(null),
        FAIL(null),
        FILE_NOT_FOUND("FileNotFoundException"),
        FILE_ALREADY_EXISTS("FileAlreadyExistsException"),
        RECOVERY_IN_PROGRESS("RecoveryInProgressException"),
        ALREADY_CREATE_EXISTS("AlreadyBeingCreatedException");

        static final EnumSet<DecodedStatus> requiresReplicationAdjustmentSet = EnumSet.of(ALREADY_CREATE_EXISTS, RECOVERY_IN_PROGRESS);

        static final Pattern hdfsExceptionRE =
                Pattern.compile("\"exception\":\"(?<exception>(?:[^\"\\\\]|\\\\.)+)");

        static final Map<String, DecodedStatus> exceptions;

        static {
            ImmutableMap.Builder<String, DecodedStatus> builder = ImmutableMap.builder();
            for (DecodedStatus drsp: values()) {
                if (drsp.exception != null) {
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
            if (rsp == null) return FAIL;
            switch (rsp.getStatusLine().getStatusCode()) {
            case HttpStatus.SC_OK:
            case HttpStatus.SC_CREATED:
            case HttpStatus.SC_ACCEPTED:
                return OK;
            case HttpStatus.SC_NOT_FOUND:
            case HttpStatus.SC_FORBIDDEN:
                DecodedStatus decoded = FAIL;
                String msg = "";
                try {
                    msg = EntityUtils.toString(rsp.getEntity(), Charsets.UTF_8);
                } catch (ParseException | IOException e) {
                    m_logger.warn("could not load response body to parse error message", e);
                }
                Matcher mtc = hdfsExceptionRE.matcher(msg);
                if (mtc.find() && exceptions.containsKey(mtc.group("exception"))) {
                    decoded = exceptions.get(mtc.group("exception"));
                }
                return decoded;
            default:
                return FAIL;
            }
        }

        boolean requiresReplicationAdjustment() {
            return requiresReplicationAdjustmentSet.contains(this);
        }
    }

    String m_endpoint = null;
    String m_avroSchemaLocation = null;
    TimeZone m_timeZone = VoltDB.REAL_DEFAULT_TIMEZONE;
    HttpMethod m_method = null;
    DecodeType m_decodeType = DecodeType.FORM;
    ContentType m_contentType = null;
    String m_secret = null;
    String m_signatureName = null;
    String m_signatureMethod = null;
    boolean m_compress = false;
    String m_blockReplication = null;
    private CloseableHttpAsyncClient m_client = HttpAsyncClients.createDefault();
    // m_batchMode is set to false in the configure() method by default
    boolean m_batchMode;
    // we track time in seconds for the benefit of unit tests
    // though normally the rollover period is given in hours
    int m_periodSecs;
    boolean m_isHdfs;
    boolean m_isHttpfs;
    boolean m_isKrb;

    private PoolingNHttpClientConnectionManager m_connManager = null;

    private Map<RollingDecoder,HttpExportDecoder> m_tableDecoders;
    private class RollingDecoder {
        public final String tableName;
        public final int partition;
        public final long generation;
        public RollingDecoder(final String t, final int p, final long g) {
            tableName = t;
            partition = p;
            generation = g;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 67 * hash + Objects.hashCode(this.tableName);
            hash = 67 * hash + this.partition;
            hash = 67 * hash + (int) (this.generation ^ (this.generation >>> 32));
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final RollingDecoder other = (RollingDecoder) obj;
            if (this.partition != other.partition) {
                return false;
            }
            if (this.generation != other.generation) {
                return false;
            }
            return Objects.equals(this.tableName, other.tableName);
        }
    }

    // each decoder runs on a separate thread, when file rolling is turned on then another thread is employed that periodically
    // changes each decorder's export path

    // timer used to roll batches
    protected ScheduledExecutorService m_ses = null;

    @Override
    public void configure(Properties config) throws Exception
    {
        m_avroSchemaLocation = config.getProperty("avro.schema.location","export/avro/%t_avro_schema.json").trim();
        if (uriRE.matcher(m_avroSchemaLocation).find() && !HDFSUtils.isHdfsUri(m_avroSchemaLocation)) {
            throw new IllegalArgumentException(
                    "remote endpoint for avro.schema.location "
                  + m_avroSchemaLocation
                  + " is not a webhdfs URL"
                  );
        }

        try {
            URI uri = new URI(EndpointExpander.expand(m_avroSchemaLocation, "CONFIGURATION_CHECK", 123L));
            final String scheme = uri.getScheme() != null ? uri.getScheme().toLowerCase() : null;
            if (scheme != null && !("http".equals(scheme) || "https".equals(scheme))) {
                throw new IllegalArgumentException("only 'http' or 'https' endpoints are supported");
            }
            if (scheme != null && HDFSUtils.isHdfsUri(uri) && HDFSUtils.containsOpQuery(uri)) {
                throw new IllegalArgumentException("avro schema location may not contain the OP query");
            }
        } catch (URISyntaxException | IllegalArgumentException e) {
            throw new IllegalArgumentException("could not expand avro schema location " + m_avroSchemaLocation, e);
        }

        m_endpoint = config.getProperty("endpoint","").trim();

        if (m_endpoint.isEmpty()) {
            throw new IllegalArgumentException("HttpExportClient: must provide an endpoint");
        }
        m_isHdfs = HDFSUtils.isHdfsUri(m_endpoint);
        m_blockReplication = config.getProperty("replication","").trim();
        if(m_isHdfs && !StringUtils.isEmpty(m_blockReplication) && !StringUtils.isNumeric(m_blockReplication)){
            throw new IllegalArgumentException("HttpExportClient: the block replication size must be an integer");
        }
        try {
            URI uri = new URI(EndpointExpander.expand(m_endpoint, "CONFIGURATION_CHECK", 0, 123L, new Date()));
            final String scheme = uri.getScheme() != null ? uri.getScheme().toLowerCase() : null;
            if (!("http".equals(scheme) || "https".equals(scheme))) {
                throw new IllegalArgumentException("only 'http' or 'https' endpoints are supported");
            }
            if (HDFSUtils.isHdfsUri(uri)) {
                EndpointExpander.verifyForHdfsUse(m_endpoint);
            }
            if (scheme != null && HDFSUtils.isHdfsUri(uri) && HDFSUtils.containsOpQuery(uri)) {
                throw new IllegalArgumentException("endpoint may not containt the OP query");
            }
        } catch (URISyntaxException | IllegalArgumentException e) {
            throw new IllegalArgumentException("could not expand endpoint " + m_endpoint, e);
        }

        String timeZoneID = config.getProperty("timezone", "").trim();
        if (!timeZoneID.isEmpty()) {
            m_timeZone = TimeZone.getTimeZone(timeZoneID);
        }

        // We track period in seconds for the benefit of unit test code; the default config unit is hours
        m_periodSecs = TimeUtils.convertIntTimeAndUnit(config.getProperty("period", "1"), TimeUnit.SECONDS, TimeUnit.HOURS);

        m_tableDecoders = Collections.synchronizedMap(new LinkedHashMap<RollingDecoder, HttpExportDecoder>());
        m_batchMode = Boolean.parseBoolean(config.getProperty("batch.mode", Boolean.toString(m_isHdfs)));

        if (m_isHdfs && !m_batchMode) {
            throw new IllegalArgumentException("HttpExportClient: only support exporting to WebHDFS in batch mode");
        }
        if (!m_isHdfs && m_batchMode) {
            EndpointExpander.verifyForBatchUse(m_endpoint);
        }

        // webhdfs enpoint is served by an Hadooop HttpFS servers
        m_isHttpfs = m_isHdfs && Boolean.parseBoolean(config.getProperty("httpfs.enable","false"));

        m_method = HttpMethod.POST;
        String method = config.getProperty("method","").trim().toUpperCase();
        if (!m_isHdfs && !method.isEmpty()) try {
            m_method = HttpMethod.valueOf(method);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("method may only be 'post','get', and 'put'");
        }

        m_secret = config.getProperty("secret");
        m_signatureMethod = config.getProperty("signatureMethod", HmacSHA1);
        m_signatureName = config.getProperty("signatureName", "Signature");

        if (!m_signatureMethod.equals(HmacSHA1) && !m_signatureMethod.equals(HmacSHA256)) {
            throw new IllegalArgumentException("HttpExportClient: only support (" +
                                               HmacSHA1 + ", " + HmacSHA256 + ") signature methods");
        }

        m_decodeType = m_batchMode ? DecodeType.CSV : DecodeType.FORM;

        String format = config.getProperty("type","").trim().toUpperCase();
        if (format.isEmpty()) {
            format = config.getProperty("contentType","").trim().toUpperCase();
        }
        if (!format.isEmpty()) try {
            m_decodeType = DecodeType.valueOf(format);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("contentType may only be 'form', 'csv', or 'avro'");
        }

        m_compress = Boolean.parseBoolean(config.getProperty("avro.compress","false"));

        if (m_batchMode) {
            if (m_method == HttpMethod.GET) {
                throw new IllegalArgumentException("HttpExportClient: GET method not supported in batch mode");
            }
            if (!BatchDecodeTypes.contains(m_decodeType)) {
                throw new IllegalArgumentException("batch mode contentType may only be 'csv', or 'avro'");
            }
        } else if (m_decodeType != DecodeType.FORM) {
            throw new IllegalArgumentException("HttpExportClient: only support 'form' content type when not in batch mode");
        }
        m_contentType = m_decodeType.contentType();

        if (m_decodeType != DecodeType.FORM && m_secret != null) {
            throw new IllegalArgumentException("HttpExportClient: only support signing for 'form' content type");
        }

        if (m_logger.isDebugEnabled()) {
            m_logger.debug("Starting HTTP export client with " + m_method + " " +
                           m_endpoint + " " + m_signatureName + "=" + m_secret);
        }

        //Dont do actual config in check mode.
        boolean configcheck = Boolean.parseBoolean(config.getProperty(ExportManager.CONFIG_CHECK_ONLY, "false"));
        if (configcheck) {
            return;
        }

        m_isKrb = Boolean.parseBoolean(config.getProperty("kerberos.enable","false"));

        if (m_isKrb) {
            m_context = new LoginContext("VoltDBService");
            m_context.login();
        }

        connect();

        if (m_isHdfs && EndpointExpander.hasDateConversion(m_endpoint)) {
            // schedule rotations every m_periodSecs seconds
            Runnable rotator = new Runnable() {
                @Override
                public void run() {
                    try {
                        roll();
                    } catch (Throwable t) {
                        m_logger.error("Roller experienced error " + Throwables.getStackTraceAsString(t));
                    }
                }
            };

            int initialDelay = m_periodSecs;
            if ((m_periodSecs % 3600) == 0) { // for periods that are multiples of one hour
                initialDelay = (60 - Calendar.getInstance().get(Calendar.MINUTE)) * 60; // synchronize on the hour
                m_logger.infoFmt("File rotator will run in %s seconds then every %s hours", initialDelay, m_periodSecs/3600);
            } else {
                m_logger.infoFmt("File rotator will run every %s seconds", m_periodSecs);
            }

            m_ses = CoreUtils.getScheduledThreadPoolExecutor("Export file rotate timer", 1, CoreUtils.SMALL_STACK_SIZE);
            m_ses.scheduleWithFixedDelay(rotator, initialDelay, m_periodSecs, TimeUnit.SECONDS);
        }
    }

    /**
     * Deprecate the current batch and create a new one. The old one will still
     * be active until all writers have finished writing their current blocks
     * to it.
     */
    void roll() {
        if (m_logger.isTraceEnabled()) {
            m_logger.trace("Rolling batch.");
        }
        Map<RollingDecoder,HttpExportDecoder> decoderMap;
        synchronized(m_tableDecoders) {
            decoderMap = ImmutableMap.copyOf(m_tableDecoders);
        }
        m_logger.info("Rolling " + decoderMap.size() + " number of data sources.");

        for (Map.Entry<RollingDecoder,HttpExportDecoder> entry : decoderMap.entrySet()) {
            entry.getValue().m_exportPath = null;
        }
    }

    /**
     * Makes an HTTP request to the HDFS to create the appropriate directories and file for the path, if it is
     * successfully created the path[0] param will be set to this new path.
     *
     * @param path      the file URI
     * @return {@link DecodedStatus} derived status from the request responses
     * @throws PathHandlingException if it encounters a problem creating the path
     */
    private DecodedStatus makePath(URI path, AbstractHttpEntity headerEntity) throws PathHandlingException {
        DecodedStatus status = DecodedStatus.FAIL;
        try {
            HttpPut dirMaker = HDFSUtils.createDirectoryRequest(new URI(path.getScheme(), path.getAuthority(),
                    path.getPath().substring(0, (path.getPath().lastIndexOf("/") + 1)),
                    path.getQuery(), path.getFragment()));
            if (m_isHttpfs) {
                dirMaker.setHeader(OctetStreamContentTypeHeader);
            }
            status = checkResponse(m_client.execute(dirMaker,null).get());
            if (status != DecodedStatus.OK) return status;
        } catch (InterruptedException|ExecutionException|URISyntaxException e) {
            m_logger.rateLimitedError(LOG_RATE_LIMIT, "error creating parent directory for %s %s", path, Throwables.getStackTraceAsString(e));
            throw new PathHandlingException("error creating parent directory for " + path, e);
        }

        try {
            HttpPut fileMaker = HDFSUtils.createFileRequest(path);
            adjustReplicationFactorForURI(fileMaker);
            if (headerEntity != null) {
                // HttpFS only accepts PUT/POST requests with application/octet-strema content type
                if (m_isHttpfs) {
                    headerEntity.setContentType(OctetStreamMimeType);
                }
                fileMaker.setEntity(headerEntity);
            } else if (m_isHttpfs) {
                fileMaker.setHeader(OctetStreamContentTypeHeader);
            }
            status = checkResponse(m_client.execute(fileMaker,null).get());
        } catch (InterruptedException | ExecutionException | URISyntaxException e) {
            m_logger.rateLimitedError(LOG_RATE_LIMIT, "error creating file %s %s", path, Throwables.getStackTraceAsString(e));
            throw new PathHandlingException("error creating file " + path,e);
        }
        if (status == DecodedStatus.FILE_ALREADY_EXISTS) {
            SimpleDateFormat dfmt = new SimpleDateFormat("yyyy-MM-dd'T'HH.mm.ss.SSS");
            dfmt.setTimeZone(m_timeZone);
            int retries = 0;
            while (status != DecodedStatus.OK && retries++ < 2) {
                try {
                    HttpGet statusGetter = HDFSUtils.createFileStatusRequest(path);
                    HttpResponse response = m_client.execute(statusGetter, null).get();
                    status = checkResponse(response);
                    if (status != DecodedStatus.OK) continue;

                    String fileStatus = EntityUtils.toString(response.getEntity(), Charsets.UTF_8);
                    Matcher mtc = modtimeRE.matcher(fileStatus);
                    if (!mtc.find()) {
                        throw new PathHandlingException("no modification time in " + fileStatus);
                    }

                    String renameTo = HDFSUtils.getHdfsPath(path) + "."
                            + dfmt.format(Long.parseLong(mtc.group("modtime")));

                    HttpPut renameDoer = HDFSUtils.createRenameRequest(path, renameTo);
                    if (m_isHttpfs) {
                        renameDoer.setHeader(OctetStreamContentTypeHeader);
                    }
                    status = checkResponse(m_client.execute(renameDoer,null).get());
                    if (status != DecodedStatus.OK) continue;

                    HttpPut fileMaker = HDFSUtils.createFileRequest(path);
                    if (headerEntity != null) {
                        if (m_isHttpfs) {
                            headerEntity.setContentType(OctetStreamMimeType);
                        }
                        fileMaker.setEntity(headerEntity);
                    } else if (m_isHttpfs) {
                        fileMaker.setHeader(OctetStreamContentTypeHeader);
                    }
                    status = checkResponse(m_client.execute(fileMaker,null).get());
                } catch (InterruptedException | ExecutionException | IOException e) {
                    m_logger.rateLimitedError(LOG_RATE_LIMIT, "error creating file %s %s", path, Throwables.getStackTraceAsString(e));
                    throw new PathHandlingException("error creating file " + path,e);
                }
            }
            if (retries >= 2) {
                throw new PathHandlingException("failed to handle file already exisits for " + path);
            }
        }

        return status;
    }

    /**
     * append replication factor to the URI for CREATE operation if the factor is not in URI
     * @param httpPut  HttpPut for REST request
     * @throws URISyntaxException  mis-formed URI
     */
    private void adjustReplicationFactorForURI(HttpPut httpPut) throws URISyntaxException{
        String queryString = httpPut.getURI().getQuery();
        if(!StringUtils.isEmpty(queryString) && queryString.contains("op=CREATE") && (queryString.contains("replication=") || !StringUtils.isEmpty(m_blockReplication))){
            m_logger.rateLimitedWarn(LOG_RATE_LIMIT, "Set block replication factor in the target system.");
            if(!StringUtils.isEmpty(m_blockReplication) && !queryString.contains("replication=")){
                StringBuilder builder = new StringBuilder(128);
                builder.append(queryString).append("&replication=").append(m_blockReplication);
                URI oldUri = httpPut.getURI();
                URI newUri = new URI(oldUri.getScheme(), oldUri.getAuthority(),oldUri.getPath(), builder.toString(), oldUri.getFragment());
                httpPut.setURI(newUri);
            }
        }
    }

    private void writeAvroSchemaToLocalFileSystem(
            ExportRow row, StringEntity schemaEntity
    ) throws PathHandlingException {
        File schemaFH = new File(EndpointExpander.expand(m_avroSchemaLocation, row.tableName, row.generation));
        File dir = schemaFH.getParentFile();
        dir.mkdirs();
        if (   !dir.exists()
            || !dir.isDirectory()
            || !dir.canRead()
            || !dir.canWrite()
            || !dir.canExecute()
        ) {
            throw new PathHandlingException("no write access to " + dir);
        }
        try (FileOutputStream fos = new FileOutputStream(schemaFH)) {
            schemaEntity.writeTo(fos);
        } catch (IOException e) {
            throw new PathHandlingException(e);
        }
    }

    private boolean writeAvroSchemaToHdfs(
            ExportRow row, StringEntity schemaEntity
    ) throws PathHandlingException {
        String filePath = EndpointExpander.expand(m_avroSchemaLocation, row.tableName, row.generation);
        URI fileURI = null;
        try {
            fileURI = new URI(filePath);
            HttpPut dirMaker = HDFSUtils.createDirectoryRequest(new URI(fileURI.getScheme(), fileURI.getAuthority(),
                    fileURI.getPath().substring(0, (fileURI.getPath().lastIndexOf("/") + 1)),
                    fileURI.getQuery(), fileURI.getFragment()));
            if (m_isHttpfs) {
                dirMaker.setHeader(OctetStreamContentTypeHeader);
            }
            Future<HttpResponse> fut = m_client.execute(dirMaker,null);

            if (checkResponse(fut.get()) != DecodedStatus.OK) return false;
        } catch (InterruptedException|ExecutionException|URISyntaxException e) {
            m_logger.rateLimitedError(LOG_RATE_LIMIT, "error creating parent directory %s", Throwables.getStackTraceAsString(e));
            throw new PathHandlingException("error creating parent directory",e);
        }
        try {
            HttpPut fileMaker = HDFSUtils.createOrOverwriteFileRequest(fileURI);
            adjustReplicationFactorForURI(fileMaker);
            if (m_isHttpfs) {
                schemaEntity.setContentType(OctetStreamMimeType);
            }
            fileMaker.setEntity(schemaEntity);
            Future<HttpResponse> fut = m_client.execute(fileMaker,null);

            if (checkResponse(fut.get()) != DecodedStatus.OK) return false;

        } catch (InterruptedException | ExecutionException | URISyntaxException e) {
            m_logger.rateLimitedError(LOG_RATE_LIMIT, "error writing avro schema file %s", Throwables.getStackTraceAsString(e));
            throw new PathHandlingException("error writing avro schema file",e);
        }
        return true;
    }

    @Override
    public void shutdown()
    {
        if (m_ses != null) {
            m_ses.shutdown();
        }

        try {
            m_client.close();
            m_connManager.shutdown(60 * 1000);
        } catch (IOException e) {
            m_logger.error("Error closing the HTTP client " + Throwables.getStackTraceAsString(e));
        }
    }

    /**
     * Calculate the signature of the request using the specified secret key.
     *
     * @param params    The parameters to send in the request.
     * @return The parameters including the signature.
     */
    private List<NameValuePair> sign(URI uri, final List<NameValuePair> params)
    {
        Preconditions.checkNotNull(m_secret);

        final List<NameValuePair> sortedParams = Lists.newArrayList(params);
        Collections.sort(sortedParams, new Comparator<NameValuePair>() {
            @Override
            public int compare(NameValuePair left, NameValuePair right)
            {
                return left.getName().compareTo(right.getName());
            }
        });
        final StringBuilder paramSb = new StringBuilder();
        String separator = "";
        for (NameValuePair param : sortedParams) {
            paramSb.append(separator).append(param.getName());
            if (param.getValue() != null) {
                paramSb.append("=").append(param.getValue());
            }
            separator = "&";
        }

        final StringBuilder baseSb = new StringBuilder();
        baseSb.append(m_method).append('\n');
        baseSb.append(uri.getHost()).append('\n');
        baseSb.append(uri.getPath().isEmpty() ? '/' : uri.getPath()).append('\n');
        baseSb.append(paramSb.toString());

        final Mac hmac;
        final Key key;
        try {
            hmac = Mac.getInstance(m_signatureMethod);
            key = new SecretKeySpec(m_secret.getBytes(Charsets.UTF_8), m_signatureMethod);
            hmac.init(key);
        } catch (NoSuchAlgorithmException e) {
            // should never happen
            m_logger.rateLimitedError(LOG_RATE_LIMIT, "Fail to get HMAC instance %s", Throwables.getStackTraceAsString(e));
            return null;
        } catch (InvalidKeyException e) {
            m_logger.rateLimitedError(LOG_RATE_LIMIT, "Fail to sign the message %s", Throwables.getStackTraceAsString(e));
            return null;
        }

        sortedParams.add(new BasicNameValuePair(m_signatureName,
                NVPairsDecoder.percentEncode(Encoder.base64Encode(hmac.doFinal(baseSb.toString().getBytes(Charsets.UTF_8))))));
        return sortedParams;
    }

    /**
     * Generate the HTTP request.
     * @param uri            The request URI
     * @param requestBody    The request body, URL encoded if necessary
     * @return The HTTP request.
     */
    private HttpUriRequest makeRequest(URI uri, final String requestBody)
    {
        HttpUriRequest request;
        if (m_method == HttpMethod.GET) {
            request = new HttpGet(uri + "?" + requestBody);
        } else if (m_method == HttpMethod.POST) {
            if (m_isHdfs) {
                try {
                    uri = HDFSUtils.opAdder(uri, "APPEND");
                } catch (IllegalArgumentException e) {
                    m_logger.rateLimitedError(LOG_RATE_LIMIT, "Invalid URI %s %s", uri.toString(), Throwables.getStackTraceAsString(e));
                    return null;
                }
            }
            final HttpPost post = new HttpPost(uri);
            post.setEntity(new StringEntity(requestBody, m_contentType));
            request = post;

        } else if (m_method == HttpMethod.PUT) {
            final HttpPut put = new HttpPut(uri);
            put.setEntity(new StringEntity(requestBody, m_contentType));
            request = put;
        } else {
            // Should never reach here
            request = null;
        }

        if (m_isHdfs && request != null) {
            request.setHeader("Expect", "100-continue");
        }

        return request;
    }

    private HttpUriRequest makeBatchRequest(URI uri, final AbstractHttpEntity enty) {
        // HttpFS only accepts PUT/POST requests with application/octet-strema content type
        if (enty != null && m_isHttpfs) {
            enty.setContentType(OctetStreamMimeType);
        }
        HttpUriRequest request;
        if (m_method == HttpMethod.POST) {
            if (m_isHdfs) {
                try {
                    uri = HDFSUtils.opAdder(uri, "APPEND");
                } catch (IllegalArgumentException e) {
                    m_logger.rateLimitedError(LOG_RATE_LIMIT, "Invalid URI %s %s", uri.toString(), Throwables.getStackTraceAsString(e));
                    return null;
                }
            }
            final HttpPost post = new HttpPost(uri);
            post.setEntity(enty);
            request = post;

        } else if (m_method == HttpMethod.PUT) {
            final HttpPut put = new HttpPut(uri);
            put.setEntity(enty);
            request = put;
        } else {
            // Should never reach here
            request = null;
        }

        if (m_isHdfs && request != null) {
            request.setHeader("Expect", "100-continue");
        }

        return request;
    }
    /**
     * Generate the HTTP request.
     * @param uri            The request URI
     * @param nvpairs        The list of name value pairs
     * @return The HTTP request.
     * @throws URISyntaxException
     */
    private HttpUriRequest makeRequest(final URI uri, final List<NameValuePair> nvpairs)
    {
        List<NameValuePair> params = nvpairs;
        if (m_secret != null) {
            params = sign(uri, nvpairs);
        }
        return makeRequest(uri, joinParameters(params));
    }

    private static String joinParameters(List<NameValuePair> params)
    {
        final StringBuilder sb = new StringBuilder();
        String separator = "";
        for (NameValuePair param : params) {
            sb.append(separator).append(param.getName());
            if (param.getValue() != null) {
                sb.append("=").append(param.getValue());
            }
            separator = "&";
        }
        return sb.toString();
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source)
    {
        HttpExportDecoder dec = new HttpExportDecoder(source);
        return dec;
    }

    // Creates the kerberos authenticated client within the login context
    class PrivilegedBuild implements PrivilegedAction{
        HttpAsyncClientBuilder m_client;

        PrivilegedBuild(HttpAsyncClientBuilder client) {
            m_client = client;
        }

        @Override
        public CloseableHttpAsyncClient run() {
            Credentials jaasCredentials = new Credentials() {
                @Override
                public String getPassword() {
                    return null;
                }

                @Override
                public Principal getUserPrincipal() {
                    return null;
                }
            };

            CredentialsProvider credsProvider = new BasicCredentialsProvider();
            credsProvider.setCredentials(new AuthScope(null, -1, null),
                    jaasCredentials);
            Registry<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder
                    .<AuthSchemeProvider> create().register(AuthSchemes.SPNEGO,
                            new SPNegoSchemeFactory(true)).build();
            return m_client.setDefaultAuthSchemeRegistry(authSchemeRegistry)
                    .setDefaultCredentialsProvider(credsProvider).build();
        }
    }

    /**
     * Construct an async HTTP client with connection pool.
     * @throws IOReactorException
     */
    private synchronized void connect() throws IOReactorException
    {
        if (m_connManager == null) {
            ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
            m_connManager = new PoolingNHttpClientConnectionManager(ioReactor);
            m_connManager.setMaxTotal(HTTP_EXPORT_MAX_CONNS);
            m_connManager.setDefaultMaxPerRoute(HTTP_EXPORT_MAX_CONNS);
        }

        if (m_client == null || !m_client.isRunning()) {
            HttpAsyncClientBuilder client = HttpAsyncClients.custom().setConnectionManager(m_connManager).
                    setRedirectStrategy(new HDFSUtils.HadoopRedirectStrategy());
            if (m_isKrb) {
                m_client = (CloseableHttpAsyncClient)Subject.doAs(m_context.getSubject(), new PrivilegedBuild(client));
            } else {
                m_client = client.build();
            }
            m_client.start();
        }
    }

    class HttpExportDecoder extends ExportDecoderBase {
        private final NVPairsDecoder m_nvpairDecoder;
        private final ListeningExecutorService m_es;
        private final List<Future<HttpResponse>> m_outstanding = Lists.newArrayList();
        public volatile URI m_exportPath;
        private boolean m_startedProcessingRows = false;

        private final EntityDecoder m_entityDecoder;
        private RollingDecoder m_rollingDecoder = null;

        @Override
        public ListeningExecutorService getExecutor() {
            return m_es;
        }

        public HttpExportDecoder(AdvertisedDataSource source)
        {
            super(source);

            if (m_batchMode) {

                EntityDecoder entityDecoder = null;

                if (m_decodeType == DecodeType.CSV) {

                    CSVEntityDecoder.Builder entityBuilder = CSVEntityDecoder.builder();
                    entityBuilder
                        .timeZone(m_timeZone)
                        .skipInternalFields(true)
                    ;
                    entityDecoder = entityBuilder.build();

                } else if (m_decodeType == DecodeType.AVRO) {

                    AvroEntityDecoder.Builder entityBuilder = AvroEntityDecoder.builder();
                    entityBuilder
                        .compress(m_compress)
                        .timeZone(m_timeZone)
                        .skipInternalFields(true)
                    ;
                    entityDecoder = entityBuilder.build();
                }
                m_entityDecoder = entityDecoder;
                m_nvpairDecoder = null;

            } else /* if (m_batchMode) */ {

                NVPairsDecoder.Builder builder = NVPairsDecoder.builder();
                builder.skipInternalFields(true);
                m_nvpairDecoder = builder.build();
                m_entityDecoder = null;
            }

            m_exportPath = null;
            if (VoltDB.getExportManager().getExportMode() == ExportMode.BASIC) {
                m_es = CoreUtils.getListeningSingleThreadExecutor(
                        "HTTP Export decoder for partition " + source.partitionId, CoreUtils.MEDIUM_STACK_SIZE);
            } else {
                m_es = null;
            }
        }

        @Override
        public boolean processRow(ExportRow row) throws RestartBlockException
        {
            URI exportPath = m_exportPath;
            if (m_client == null || !m_client.isRunning()) {
                try {
                    connect();
                } catch (IOReactorException e) {
                    m_logger.rateLimitedError(LOG_RATE_LIMIT, "Unable to create HTTP client %s", Throwables.getStackTraceAsString(e));
                    throw new RestartBlockException(true);
                }
            }
            if (!m_startedProcessingRows) try {
                if (m_isHdfs) {
                    DecodedStatus status = makePath(exportPath, getHeaderEntity(row));
                    if (status != DecodedStatus.OK) {
                        throw new PathHandlingException("hdfs makePath returned false for " + exportPath);
                    }
                }
                writeAvroSchema(row);
                m_startedProcessingRows = true;
            } catch (PathHandlingException e) {
                    m_logger.rateLimitedError(LOG_RATE_LIMIT, "Unable to prime http export client to %s %s", exportPath, Throwables.getStackTraceAsString(e));
                throw new RestartBlockException(true);
            }

            HttpUriRequest rqst;

            if (m_decodeType == DecodeType.FORM) {
                try {
                    rqst = makeRequest(exportPath, m_nvpairDecoder.decode(row.generation, row.tableName, row.types, row.names, null, row.values));
                } catch (RuntimeException e) {
                    // non restartable structural failure
                    m_logger.rateLimitedError(LOG_RATE_LIMIT, "unable to build an HTTP request from an exported row %s", Throwables.getStackTraceAsString(e));
                    return false;
                }
            } else if (m_batchMode) {
                try {
                    m_entityDecoder.add(row.generation, row.tableName, row.types, row.names, row.values);
                    return true;
                } catch (RuntimeException e) {
                    // non restartable structural failure
                    m_logger.rateLimitedError(LOG_RATE_LIMIT, "unable to acummulate export records in batch mode %s", Throwables.getStackTraceAsString(e));
                    return false;
                }
            } else {
                // we should not get here as this case would throw at a configure
                throw new RuntimeException("Non-batch CSV, or Avro format are not supported yet");
            }

            try {
                m_outstanding.add(m_client.execute(rqst, null));
            } catch (Exception e) {
                // May be recoverable, retry with a backoff
                m_logger.rateLimitedError(LOG_RATE_LIMIT, "Unable to dispatch a request to \"%s\". Reason:\n%s", rqst, Throwables.getStackTraceAsString(e));
                throw new RestartBlockException(true);
            }

            return true;
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source)
        {
            if ( (m_isHdfs || m_decodeType == DecodeType.AVRO) && m_rollingDecoder != null) {
                m_tableDecoders.remove(m_rollingDecoder);
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

        @Override
        public void onBlockStart(ExportRow row) throws RestartBlockException
        {
            m_outstanding.clear();
            if (m_exportPath == null) {
                final String endpoint = EndpointExpander.expand(
                        m_endpoint,
                        row.tableName,
                        row.partitionId,
                        row.generation,
                        new Date(),
                        m_timeZone
                        );
                try {
                    m_exportPath = new URI(endpoint);
                } catch (URISyntaxException e) {
                    // should not get here as the endpoint URL syntax was validated at configure
                    m_logger.error("Unable to create URI " + endpoint + " " + Throwables.getStackTraceAsString(e));
                    m_exportPath = null;
                    throw new RestartBlockException(true);
                }
            }
            if (m_isHdfs || m_decodeType == DecodeType.AVRO) {
                m_rollingDecoder = new RollingDecoder(row.tableName, row.partitionId, row.generation);
                m_tableDecoders.put(m_rollingDecoder, this);
            }
        }

        @Override
        public void onBlockCompletion(ExportRow row) throws RestartBlockException
        {
            final URI exportPath = m_exportPath;
            if (m_batchMode) {
                HttpUriRequest rqst = null;
                try {
                    rqst = makeBatchRequest(
                            exportPath, m_entityDecoder.harvest(row.generation)
                            );
                    Future<HttpResponse> fut = m_client.execute(rqst, null);

                    DecodedStatus status = checkResponse(fut.get());
                    if (status == DecodedStatus.FILE_NOT_FOUND) {
                        makePath(exportPath, getHeaderEntity(row));
                    }
                    String queryString = rqst.getURI().getQuery();
                    if (queryString.contains("op=APPEND") && status.requiresReplicationAdjustment()) {
                        m_logger.rateLimitedWarn(LOG_RATE_LIMIT, "error in appending data to block. System is trying to set block replication size to 1. Please verify the configurations in the target export file system.");
                        try{
                            HttpPut replicationPutter = HDFSUtils.createSetReplicationRequest(exportPath, 1);
                            HttpResponse response = m_client.execute(replicationPutter, null).get();
                            status = checkResponse(response);
                            if (status != DecodedStatus.OK){
                                 m_logger.rateLimitedError(LOG_RATE_LIMIT, "error set replication size 1 for %s", exportPath);
                            }else{
                                 throw new RestartBlockException("requeing after replication reset",true);
                            }
                        }catch (InterruptedException | ExecutionException  e) {
                            m_logger.rateLimitedError(LOG_RATE_LIMIT, "error set replication size %s %s", exportPath, Throwables.getStackTraceAsString(e));
                            throw e;
                        }
                    }
                    if (status != DecodedStatus.OK) {
                        throw new RestartBlockException("requeing on failed response check: " + status, true);
                    }
                } catch (Exception e) {
                    // May be recoverable, retry with a backoff'
                    m_logger.rateLimitedError(LOG_RATE_LIMIT,
                            "Unable to complete request to \"%s\". Reason:\n%s",
                            rqst, Throwables.getStackTraceAsString(e)
                            );
                    throw new RestartBlockException(true);
                }
            }

            for (Future<HttpResponse> request : m_outstanding) {
                try {
                    if (checkResponse(request.get()) != DecodedStatus.OK) {
                        throw new RestartBlockException("requeing on failed response check", true);
                    }
                } catch (Exception e) {
                    m_logger.rateLimitedError(LOG_RATE_LIMIT, "Failure reported in request response. Reason:\n%s", Throwables.getStackTraceAsString(e));
                    throw new RestartBlockException(true);
                }
            }
        }

        public AbstractHttpEntity getHeaderEntity(ExportRow row) {
            return m_entityDecoder != null ? m_entityDecoder.getHeaderEntity(row.generation, row.tableName, row.types, row.names) : null;
        }

        /**
         * Writes out the Avro schema JSON document the location specified in
         * {@linkplain HttpExportClient#m_avroSchemaLocation}. This decoder
         * becomes the actual writer if it is partition 0 and the schema location
         * is a webdhdfs URL, or if it is the first decoder initialized for this
         * host and generation.
         *
         * @throws PathHandlingException when it cannot write the schema to
         *         the configured endpoint
         */
        public void writeAvroSchema(ExportRow row) throws PathHandlingException {
            if (m_decodeType == DecodeType.AVRO) {

                boolean isHdfs = HDFSUtils.isHdfsUri(m_avroSchemaLocation);

                RollingDecoder first = null;
                ImmutableList<RollingDecoder> sources =
                        ImmutableList.copyOf(m_tableDecoders.keySet());

                Iterator<RollingDecoder> sourceIter = sources.iterator();
                while (sourceIter.hasNext() && first == null) {
                    RollingDecoder current = sourceIter.next();
                    if (
                            current.generation == row.generation
                         && (!isHdfs || (current.partition == 0))
                    ) {
                        first = current;
                    }
                }

                if (first == null) return;
                StringEntity enty = ((AvroEntityDecoder)m_entityDecoder).getSchemaAsEntity(row.generation, row.tableName, row.types, row.names);
                if (isHdfs) {
                    writeAvroSchemaToHdfs(row, enty);
                } else {
                    writeAvroSchemaToLocalFileSystem(row, enty);
                }
//TODO
//                if (m_source.equals(first)) {
//                    StringEntity enty = ((AvroEntityDecoder)m_entityDecoder).getSchemaAsEntity(row.generation, row.tableName, row.types, row.names);
//                    if (isHdfs) {
//                        writeAvroSchemaToHdfs(row, enty);
//                    } else {
//                        writeAvroSchemaToLocalFileSystem(row, enty);
//                    }
//                }
            }
        }
    }

    DecodedStatus checkResponse(HttpResponse response)
    {
        if (m_logger.isTraceEnabled() && response.getEntity().getContentLength() > 0) {
            try {
                String msg = EntityUtils.toString(response.getEntity(), Charsets.UTF_8);
                m_logger.trace("Notification response: " + msg);
            } catch (IOException e) {
                m_logger.warn("could not trace response body",e);
            }
        }

        DecodedStatus status = DecodedStatus.fromResponse(response);
        if (status == DecodedStatus.FAIL) {
            if (m_isHdfs) {
                try {
                    String msg = EntityUtils.toString(response.getEntity(), Charsets.UTF_8);
                    m_logger.rateLimitedError(LOG_RATE_LIMIT, "Notification request failed with %s.\nNotification response: %s", response.getStatusLine().toString(), msg);
                } catch (IOException e) {
                    m_logger.error("could not trace response body",e);
                }
            }
            else {
                m_logger.rateLimitedError(LOG_RATE_LIMIT, "Notification request failed with %s", response.getStatusLine().toString());
            }
        }
        return status;
    }

    static public class PathHandlingException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public PathHandlingException(String message, Throwable cause)
        {
            super(message, cause);
        }

        public PathHandlingException(Throwable cause)
        {
            super(cause);
        }

        public PathHandlingException(String message)
        {
            super(message);
        }
    }
}
