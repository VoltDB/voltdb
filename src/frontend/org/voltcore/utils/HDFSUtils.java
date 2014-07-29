package org.voltcore.utils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.*;
import org.voltcore.logging.VoltLogger;

public class HDFSUtils {
    VoltLogger m_logger;

    public HDFSUtils(VoltLogger logger) {
        m_logger = logger;
    }

    private URI opAdder(URI url, String op) {
        String scheme = url.getScheme();
        String authority = url.getAuthority();
        String path = url.getPath();
        String query = url.getQuery();
        String fragment = url.getFragment();
        if (query != null) {
            if (query.matches("op=(.*)") || query.matches("(.*)&op=(.*)")) {
                m_logger.error("Invalid URI: Query cannot contain op field");
                return null;
            }
            else {
                query = "op=" + op + "&" + query;
            }
        }
        else {
            query = "op=" + op;
        }
        try {
            url = new URI(scheme, authority, path, query, fragment);
        } catch (URISyntaxException e) {
            m_logger.error("URI Syntax Exception", e);
            return null;
        }
        return url;
    }

    public Boolean createFile(URI url, HttpClient client) {
        if ((url = opAdder(url, "CREATE")) == null) {
            return false;
        }
        HttpPut request = new HttpPut(url);
        try {
            HttpResponse response = client.execute(request);

            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_TEMPORARY_REDIRECT) {
                request = new HttpPut(response.getFirstHeader("Location").getValue());
                response = client.execute(request);
            }

            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                m_logger.error("Http Request Failed: " + response.getStatusLine().toString());
                return false;
            }
        } catch (ClientProtocolException e) {
            m_logger.error("Protocol exception", e);
            return false;
        } catch (IOException e) {
            m_logger.error("I/O exception", e);
            return false;
        }
        return true;
    }

    public Boolean createDirectory(URI url, HttpClient client) {
        if ((url = opAdder(url, "MKDIRS")) == null) {
            return false;
        }
        HttpPut request = new HttpPut(url);
        try {
            HttpResponse response = client.execute(request);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                m_logger.error("Http Request Failed: " + response.getStatusLine().toString());
                return false;
            }
        } catch (ClientProtocolException e) {
            m_logger.error("Protocol exception", e);
            return false;
        } catch (IOException e) {
            m_logger.error("I/O exception", e);
            return false;
        }
        return true;
    }
}
