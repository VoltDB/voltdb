# How to Enable SSL for Client Connections

This document demonstrates how to configure and run VoltDB while using SSL to secure connections between the database and clients using the VoltDB API.

VoltDB servers using SSL must be provided with a private key and a certificate which establishes trust for that key. Clients, including 'sqlcmd', are provided one or more certificates that establish that the server can be trusted.

Please note that enabling SSL introduces additional overhead. This can reduce the maximum throughput and increase the latency of each client’s connection to the database, which may affect your application.

Both the "Voter" and “VoltKV” example clients support SSL.

## Prerequisites

For security and performance reasons, it is important to ensure that both your JVM and your client support the encryption protocol you are looking to use.

You may need to install Oracle’s "Unlimited Strength Java(TM) Cryptography Extension Policy" package to use more secure encryption schemes, or a comparable substitute such as Bouncy Castle. 

[http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html](http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html)

http://www.bouncycastle.org/latest_releases.html

VoltDB recommends using TLS 1.2 at the minimum. When using older versions of Python, including Python 2.6 and 2.7, this is not supported by default. VoltDB supports TLS 1.0 and up to accommodate Python, but consider looking for a third-party implementation of TLS 1.2 or upgrading to Python 3.

## Setting Up the Key and Trust Stores

Before running VoltDB with SSL for the first time, you will need to configure your key and certificate stores. Both the key store and trust store must be in the 'Java Keystore' format. Generating these stores requires access to a JDK (Java Development Kit).

To facilitate testing, both self-signed and externally trusted keys may be used. VoltDB recommends that all keys used in production be kept secret and verified by your organization’s certificate authority.

Here's a quick way to generate a self-signed certificate for testing VoltDB with SSL. It can easily be adapted to use production keys and signed certificates. For a more detailed description of the process, see the following:

[https://docs.oracle.com/cd/E19509-01/820-3503/6nf1il6er/index.html](https://docs.oracle.com/cd/E19509-01/820-3503/6nf1il6er/index.html)


### Generate a keystore containing both public and private keys
```
keytool -genkey -keystore example.keystore -storepass example -alias example -keyalg rsa -validity 365 -keysize 2048
```
### Generate a key signing request
```
keytool -certreq -keystore example.keystore -storepass example -alias example -keyalg rsa -file example.csr
```
### Self-sign the key
```
keytool -gencert -keystore example.keystore -storepass example -alias example -infile example.csr -outfile example.cert -validity 365
```
### Import the certificate into the key store
```
keytool -import -keystore example.keystore -storepass example -alias example -file example.cert
```
### Create the trust store
```
keytool -import -file example.cert -keystore example.truststore -storepass example -alias example
```
## Configuring the Database
Once you have created your key store and trust store, add the following to your deployment.xml:

```xml
    <ssl enabled="true" external="true">
        <keystore   path="example.keystore"   password="example"/>
        <truststore path="example.truststore" password="example"/>
    </ssl>
```

## Enabling SSL in your Client Application

Add the following in your Java client applications:

```
   clientConfig.setTrustStore(pathToTrustStore, trustStorePassword);
   clientConfig.enableSSL();
```

Other SSL properties must be set using the JVM command line. For example, this forces TLS version 1.2:
```
user@machine:~/workspace/voltdb/examples/voter$ source ~/workspace/voltdb/bin/voltenv 
user@machine:~/workspace/voltdb/examples/voter$ java -classpath voter-client.jar:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J -Djdk.tls.client.protocols=TLSv1.2 voter.SimpleBenchmark localhost
```


## Enabling SSL in "sqlcmd" and “voltadmin”

Add the --ssl=SSL.properties argument to your command line.

```
   sqlcmd --ssl=SSL.properties < ddl.sql</td>
   voltadmin shutdown --ssl=SSL.properties</td>
```

‘SSL.properties’ is a text file which Java can interpret, and may be found in the example/HOWTOs folder. It contains the following:

```
   # You will need to change these to match your environment.
   trustStore=example.truststore
   trustStorePassword=example</td>
```

