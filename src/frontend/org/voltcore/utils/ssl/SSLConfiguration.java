/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;

/**
 * Code common to ServerSSLEngineFactory and ClientSSLEngineFactory.
 */
public class SSLConfiguration {

    public static final String KEYSTORE_CONFIG_PROP = "keyStore";
    public static final String KEYSTORE_PASSWORD_CONFIG_PROP = "keyStorePassword";
    public static final String TRUSTSTORE_CONFIG_PROP = "trustStore";
    public static final String TRUSTSTORE_PASSWORD_CONFIG_PROP = "trustStorePassword";

    // override the specified values using any provided system properties
    public static void applySystemProperties(SslConfig sslConfig) {

        String keyStorePath = System.getProperty("javax.net.ssl.keyStore");
        if (keyStorePath != null) {
            sslConfig.keyStorePath = keyStorePath;
        }
        String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
        if (keyStorePassword != null) {
            sslConfig.keyStorePassword = keyStorePassword;
        }
        String trustStorePath = System.getProperty("javax.net.ssl.trustStore");
        if (trustStorePath != null) {
            sslConfig.trustStorePath = trustStorePath;
        }
        String trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
        if (trustStorePassword != null) {
            sslConfig.trustStorePassword = trustStorePassword;
        }
    }

    public static SSLContext initializeSslContext(SslConfig sslConfig)
            throws Exception {

        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(new FileInputStream(sslConfig.keyStorePath), sslConfig.keyStorePassword.toCharArray());
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(ks, sslConfig.keyStorePassword.toCharArray());
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(createKeyManagers(sslConfig.keyStorePath, sslConfig.keyStorePassword, sslConfig.keyStorePassword),
                createTrustManagers(sslConfig.trustStorePath, sslConfig.trustStorePassword), new SecureRandom());
        return sslContext;
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
    private static KeyManager[] createKeyManagers(String filepath, String keystorePassword, String keyPassword) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        InputStream keyStoreIS = new FileInputStream(filepath);
        try {
            keyStore.load(keyStoreIS, keystorePassword.toCharArray());
        } finally {
            if (keyStoreIS != null) {
                keyStoreIS.close();
            }
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyPassword.toCharArray());
        return kmf.getKeyManagers();
    }

    /**
     * Creates the trust managers required to initiate the {@link SSLContext}, using a JKS keystore as an input.
     *
     * @param filepath - the path to the JKS keystore.
     * @param keystorePassword - the keystore's password.
     * @return {@link TrustManager} array, that will be used to initiate the {@link SSLContext}.
     * @throws Exception
     */
    private static TrustManager[] createTrustManagers(String filepath, String keystorePassword) throws Exception {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        InputStream trustStoreIS = new FileInputStream(filepath);
        try {
            trustStore.load(trustStoreIS, keystorePassword.toCharArray());
        } finally {
            if (trustStoreIS != null) {
                trustStoreIS.close();
            }
        }
        TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trustStore);
        return trustFactory.getTrustManagers();
    }

    public static String parseValue(String property, String key) {
        // strip off the key
        String restOfProperty = property.substring(key.length());
        restOfProperty.trim();
        if (! restOfProperty.startsWith("=")) {
            throw new IllegalArgumentException("Badly formed property in ssl config file, expecting '=' " + property);
        }
        // strip off the equals sign
        String value = restOfProperty.substring(1);
        value.trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException("No value specified for property in ssl config file " + property);
        }
        return value;
    }

    public static class SslConfig {
        String keyStorePath = null;
        String keyStorePassword = null;
        String trustStorePath = null;
        String trustStorePassword = null;

        public SslConfig(String keyStorePath, String keyStorePassword, String trustStorePath, String trustStorePassword) {
            this.keyStorePath = keyStorePath;
            this.keyStorePassword = keyStorePassword;
            this.trustStorePath = trustStorePath;
            this.trustStorePassword = trustStorePassword;
        }

        public boolean isValid() {
            // verify that either all the properties are null or all are set
            if (keyStorePath == null && keyStorePassword == null && trustStorePath == null && trustStorePassword == null) {
                // none of the properties are set, this is a valid config.
                return true;
            } else {
                if (isConfigured()) {
                    // all of the properties are set, this is a valid config.
                    return true;
                } else {
                    // not valid
                    return false;
                }
            }
        }

        public boolean isConfigured() {
            return (keyStorePath != null && keyStorePassword != null && trustStorePath != null && trustStorePassword != null);
        }
    }
}
