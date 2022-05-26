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

package org.voltdb;

import org.voltcore.logging.VoltLogger;
import org.voltcore.network.CipherExecutor;
import org.voltcore.utils.ssl.SSLConfiguration;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.KeyOrTrustStoreType;
import org.voltdb.compiler.deploymentfile.SslType;
import org.voltdb.common.Constants;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.URL;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;
import org.eclipse.jetty.util.security.Password;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * Set up TLS/SSL for RealVoltDB.
 */
public class SslSetup {

    private VoltLogger m_log;
    private VoltDB.Configuration m_config;
    private DeploymentType m_deployment;

    private SslSetup(VoltDB.Configuration config, DeploymentType deployment) {
        m_log = new VoltLogger("HOST");
        m_config = config;
        m_deployment = deployment;
    }

    public static void setupSSL(VoltDB.Configuration config, DeploymentType deployment) {
        SslSetup instance = new SslSetup(config, deployment);
        instance.setupSSL(instance.m_deployment.getSsl());
    }

    private void setupSSL(SslType sslType) {
        m_config.m_sslEnable = m_config.m_sslEnable || (sslType != null && sslType.isEnabled());
        if (m_config.m_sslEnable) {
            try {
                m_config.m_sslExternal |= (sslType != null && sslType.isExternal());
                m_config.m_sslDR |= (sslType != null && sslType.isDr());
                m_config.m_sslInternal |= (sslType != null && sslType.isInternal());
                setupSslContextCreators(sslType, m_config.m_sslExternal | m_config.m_sslDR | m_config.m_sslInternal);

                m_log.info("SSL enabled for HTTP. Please point browser to HTTPS URL.");
                if (m_config.m_sslExternal) {
                    m_log.info("SSL enabled for admin and client port. Please enable SSL on client.");
                }
                if (m_config.m_sslDR) {
                    m_log.info("SSL enabled for DR port. Please enable SSL on consumer clusters' DR connections.");
                }
                if (m_config.m_sslInternal) {
                    m_log.info("SSL enabled for internal inter-node communication.");
                }

                CipherExecutor.SERVER.startup();
            } catch (Exception e) {
                m_log.fatal("Exception when attempting to configure SSL", e);
                VoltDB.crashLocalVoltDB("Unable to configure SSL", true, e);
            }
        }
    }

    private void setupSslContextCreators(SslType sslType, boolean setSslBuilder) {
        SslContextFactory sslContextFactory = new SslContextFactory.Server();

        String keyStorePath = getStorePath("javax.net.ssl.keyStore", sslType.getKeystore(), Constants.DEFAULT_KEYSTORE_RESOURCE, "keystore");
        String keyStorePassword = getStorePassword("javax.net.ssl.keyStorePassword", sslType.getKeystore(),  Constants.DEFAULT_KEYSTORE_PASSWD);
        sslContextFactory.setKeyStorePath(keyStorePath);
        sslContextFactory.setKeyStorePassword(keyStorePassword);

        String trustStorePath = getStorePath("javax.net.ssl.trustStore", sslType.getTruststore(), Constants.DEFAULT_TRUSTSTORE_RESOURCE, "truststore");
        String trustStorePassword = getStorePassword("javax.net.ssl.trustStorePassword", sslType.getTruststore(),  Constants.DEFAULT_TRUSTSTORE_PASSWD);
        sslContextFactory.setTrustStorePath(trustStorePath);
        sslContextFactory.setTrustStorePassword(trustStorePassword);

        // exclude weak ciphers
        sslContextFactory.setExcludeCipherSuites(SSLConfiguration.EXCLUDED_CIPHERS.toArray(new String[0]));
        sslContextFactory.setKeyManagerPassword(keyStorePassword);

        m_config.m_sslContextFactory = sslContextFactory;

        if (setSslBuilder) {

            KeyManagerFactory keyManagerFactory;
            try (FileInputStream fis = new FileInputStream(keyStorePath)) {
                keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                keyStorePassword = deobfuscateIfNeeded(keyStorePassword);
                keyStore.load(fis, keyStorePassword.toCharArray());
                keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());
            } catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException | IOException
                    | CertificateException e) {
                throw new IllegalArgumentException("Could not initialize KeyManagerFactory", e);
            }

            TrustManagerFactory trustManagerFactory;
            try (FileInputStream fis = new FileInputStream(trustStorePath)) {
                trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                trustStorePassword = deobfuscateIfNeeded(trustStorePassword);
                keyStore.load(fis, trustStorePassword.toCharArray());
                trustManagerFactory.init(keyStore);
            } catch (NoSuchAlgorithmException | IOException | KeyStoreException | CertificateException e) {
                throw new IllegalArgumentException("Could not initialize TrustManagerFactory", e);
            }

            try {
                m_config.m_sslServerContext = SslContextBuilder.forServer(keyManagerFactory)
                                                               .trustManager(trustManagerFactory)
                                                               .ciphers(null, SSLConfiguration.CIPHER_FILTER)
                                                               .clientAuth(ClientAuth.NONE)
                                                               .build();
                m_config.m_sslClientContext = SslContextBuilder.forClient()
                                                               .trustManager(trustManagerFactory)
                                                               .ciphers(null, SSLConfiguration.CIPHER_FILTER)
                                                               .clientAuth(ClientAuth.NONE)
                                                               .build();
            } catch (SSLException e) {
                throw new IllegalArgumentException("Could not create SslContexts", e);
            }
        }
    }

    private String getStorePath(String sysPropName, KeyOrTrustStoreType store, String defaultResource, String what) {
        String resource = System.getProperty(sysPropName, "").trim();
        if (store != null) {
            if (resource.isEmpty()) {  // no system property, try deployment file value
                resource = store.getPath();
            }
            else { // prefer system property value over deployment value
                String value = store.getPath();
                if (value != null && !value.equals(resource)) {
                    m_log.info(String.format("System property '%s' overrides deployment-file value", sysPropName));
                }
            }
        }
        if (resource == null || resource.isEmpty()) {
            resource = defaultResource;
        }

        String storePath = getResourcePath(resource);
        if (storePath == null || storePath.trim().isEmpty()) {
            throw new IllegalArgumentException(String.format("A path for the SSL %s file was not specified.", what));
        }
        if (!new File(storePath).exists()) {
            throw new IllegalArgumentException(String.format("The specified SSL %s file '%s' was not found.", what, storePath));
        }
        return storePath;
    }

    private String getStorePassword(String sysPropName, KeyOrTrustStoreType store, String defaultPasswd) {
        String passwd = System.getProperty(sysPropName, "");  // password is not trimmed
        if (store != null) {
            if (passwd.isEmpty()) {  // no system property, try deployment file value
                passwd = store.getPassword();
            }
            else { // prefer system property value over deployment value
                String value = store.getPassword();
                if (value != null && !value.equals(passwd)) {
                    m_log.info(String.format("System property '%s' overrides deployment-file value", sysPropName));
                }
            }
        }
        if (passwd == null || passwd.isEmpty()) {
            passwd = defaultPasswd;
        }
        return passwd;
    }

    private String getResourcePath(String resource) {
        URL res = this.getClass().getResource(resource);
        return res == null ? resource : res.getPath();
    }

    private String deobfuscateIfNeeded(String password) {
        if (password.startsWith(Password.__OBFUSCATE)) {
            return Password.deobfuscate(password);
        }
        return password;
    }
}
