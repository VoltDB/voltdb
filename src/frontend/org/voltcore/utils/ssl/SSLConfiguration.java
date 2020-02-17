/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

package org.voltcore.utils.ssl;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Set;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.google_voltpatches.common.collect.ImmutableSet;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

/**
 * Code common to ServerSSLEngineFactory and ClientSSLEngineFactory.
 */
public class SSLConfiguration {

    public static final String KEYSTORE_CONFIG_PROP = "keyStore";
    public static final String KEYSTORE_PASSWORD_CONFIG_PROP = "keyStorePassword";
    public static final String TRUSTSTORE_CONFIG_PROP = "trustStore";
    public static final String TRUSTSTORE_PASSWORD_CONFIG_PROP = "trustStorePassword";

    public static Set<String> PREFERRED_CIPHERS = ImmutableSet.<String>builder()
            .add("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256")
            .add("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384")
            .add("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384")
            .add("TLS_RSA_WITH_AES_256_GCM_SHA384")
            .add("TLS_ECDH_ECDSA_WITH_AES_256_GCM_SHA384")
            .add("TLS_ECDH_RSA_WITH_AES_256_GCM_SHA384")
            .add("TLS_DHE_RSA_WITH_AES_256_GCM_SHA384")
            .add("TLS_DHE_DSS_WITH_AES_256_GCM_SHA384")
            .add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256")
            .add("TLS_RSA_WITH_AES_128_GCM_SHA256")
            .add("TLS_ECDH_ECDSA_WITH_AES_128_GCM_SHA256")
            .add("TLS_ECDH_RSA_WITH_AES_128_GCM_SHA256")
            .add("TLS_DHE_RSA_WITH_AES_128_GCM_SHA256")
            .add("TLS_DHE_DSS_WITH_AES_128_GCM_SHA256")
            .add("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA")
            .add("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA")
            .add("TLS_RSA_WITH_AES_256_CBC_SHA")
            .add("TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA")
            .add("TLS_ECDH_RSA_WITH_AES_256_CBC_SHA")
            .add("TLS_DHE_RSA_WITH_AES_256_CBC_SHA")
            .add("TLS_DHE_DSS_WITH_AES_256_CBC_SHA")
            .add("TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA")
            .add("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA")
            .add("TLS_RSA_WITH_AES_128_CBC_SHA")
            .add("TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA")
            .add("TLS_ECDH_RSA_WITH_AES_128_CBC_SHA")
            .add("TLS_DHE_RSA_WITH_AES_128_CBC_SHA")
            .add("TLS_DHE_DSS_WITH_AES_128_CBC_SHA")
            .build();

    public static Set<String> GCM_CIPHERS = ImmutableSet.<String>builder()
            .add("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256")
            .add("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384")
            .add("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384")
            .add("TLS_RSA_WITH_AES_256_GCM_SHA384")
            .add("TLS_ECDH_ECDSA_WITH_AES_256_GCM_SHA384")
            .add("TLS_ECDH_RSA_WITH_AES_256_GCM_SHA384")
            .add("TLS_DHE_RSA_WITH_AES_256_GCM_SHA384")
            .add("TLS_DHE_DSS_WITH_AES_256_GCM_SHA384")
            .add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256")
            .add("TLS_RSA_WITH_AES_128_GCM_SHA256")
            .add("TLS_ECDH_ECDSA_WITH_AES_128_GCM_SHA256")
            .add("TLS_ECDH_RSA_WITH_AES_128_GCM_SHA256")
            .add("TLS_DHE_RSA_WITH_AES_128_GCM_SHA256")
            .add("TLS_DHE_DSS_WITH_AES_128_GCM_SHA256")
            .build();

    public static SslContext createClientSslContext(SslConfig sslConfig) {
        if (sslConfig == null) {
            throw new IllegalArgumentException("sslConfig is null");
        }

        try {
            SslContextBuilder builder = SslContextBuilder.forClient();

            if (sslConfig.keyStorePath != null && sslConfig.keyStorePassword != null) {
                builder.keyManager(createKeyManagers(sslConfig.keyStorePath, sslConfig.keyStorePassword,
                        sslConfig.keyStorePassword));
            }
            if (sslConfig.trustStorePath != null && sslConfig.trustStorePassword != null) {
                builder.trustManager(createTrustManagers(sslConfig.trustStorePath, sslConfig.trustStorePassword));
            }

            return builder.build();
        } catch (IOException | NoSuchAlgorithmException | KeyStoreException | CertificateException
                | UnrecoverableKeyException ex) {
            throw new IllegalArgumentException("Failed to initialize SSL using " + sslConfig, ex);
        }
    }

    /**
     * Creates the key managers required to initiate the {@link SSLContext}, using a JKS keystore as an input.
     *
     * @param filepath - the path to the JKS keystore.
     * @param keystorePassword - the keystore's password.
     * @param keyPassword - the key's passsword.
     * @return {@link KeyManager} array that will be used to initiate the {@link SSLContext}.
     * @throws Exception
     */
    private static KeyManagerFactory createKeyManagers(String filepath, String keystorePassword, String keyPassword)
            throws FileNotFoundException, KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (InputStream keyStoreIS = new FileInputStream(filepath)) {
            keyStore.load(keyStoreIS, keystorePassword.toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyPassword.toCharArray());
        return kmf;
    }

    /**
     * Creates the trust managers required to initiate the {@link SSLContext}, using a JKS keystore as an input.
     *
     * @param filepath - the path to the JKS keystore.
     * @param keystorePassword - the keystore's password.
     * @return {@link TrustManager} array, that will be used to initiate the {@link SSLContext}.
     * @throws Exception
     */
    private static TrustManagerFactory createTrustManagers(String filepath, String keystorePassword)
            throws KeyStoreException, FileNotFoundException,
            IOException, NoSuchAlgorithmException, CertificateException {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        try (InputStream trustStoreIS = new FileInputStream(filepath)) {
            trustStore.load(trustStoreIS, keystorePassword.toCharArray());
        }
        TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trustStore);
        return trustFactory;
    }

    public static class SslConfig {

        public final String keyStorePath;
        public final String keyStorePassword;
        public final String trustStorePath;
        public final String trustStorePassword;

        public SslConfig() {
            this(null,null,null,null);
        }

        public SslConfig(String keyStorePath, String keyStorePassword, String trustStorePath, String trustStorePassword) {

            String pval = System.getProperty("javax.net.ssl.keyStore");
            if (pval != null) {
                keyStorePath = pval;
            }
            pval = System.getProperty("javax.net.ssl.keyStorePassword");
            if (pval != null) {
                keyStorePassword = pval;
            }
            pval = System.getProperty("javax.net.ssl.trustStore");
            if (pval != null) {
                trustStorePath = pval;
            }
            pval = System.getProperty("javax.net.ssl.trustStorePassword");
            if (pval != null) {
                trustStorePassword = pval;
            }
            this.keyStorePath = keyStorePath;
            this.keyStorePassword = keyStorePassword;
            this.trustStorePath = trustStorePath;
            this.trustStorePassword = trustStorePassword;
        }

        @Override
        public String toString() {
            return "SslConfig [keyStorePath=" + keyStorePath
                    + ", trustStorePath=" + trustStorePath + "]";
        }
    }


}
