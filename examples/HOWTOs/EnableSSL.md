# How to Enable SSL

VoltDB with SSL demonstrates how to configure and run VoltDB while using SSL to secure connections to the database.

VoltDB servers using SSL must be provided with a private key and a certificate which establishes trust for that key. Clients, including 'sqlcmd', are provided one or more certificates that establishes that the server can be trusted.

Please note that this will reduce performance due to additional overhead associated with SSL. The exact results will vary depending on the encryption protocol chosen and usage pattern, so be sure to run benchmarks.


Preparation
---------------------------

Before running VoltDB with SSL for the first time, you will need to configure your key and certificate stores. Both the key store and trust store must be in Java's 'Java Keystore' format for VoltDB and its clients to use them. Generating these stores requires access to a JDK (Java Development Kit).

- The server finds its key and certificate stores in the deployment.xml configuration file.
- Clients use a certificate store, which may be specified programmatically or via a Java 'properties' file.  
- `sqlcmd` uses a plain-text Java 'properties' file that direct them to a certificate store, such as the supplied 'SSL.properties'.

Verify that both your JVM and your client support the encryption protocol you are looking to use. You may need to install additional packages to meet your security and performance goals. Oracle Java (as of Java 8) requires the "Unlimited Strength Java(TM) Cryptography Extension Policy" package to use more secure encryption schemes, or a comparable substitute such as Bouncy Castle. The supplied client will warn you if this is not installed. If you're using Python 2, be aware that it does not support TLS 1.2 - the minimum version recommended by VoltDB. Keep in mind that some governments restrict the export and use of cryptography, and it is your responsibility to abide by any laws that apply.

Here's a quick way to generate a self-signed certificate for testing VoltDB with SSL. Using a self-signed certificate is not recommended for production or sensitive test data. For a more detailed description of the process, see the following:
https://docs.oracle.com/cd/E19509-01/820-3503/6nf1il6er/index.html

The supplied SSL.properties and deployment.xml assume all files are in this directory and all passwords are 'example'. Modify them if this is not true.

> keytool -genkey  -keystore example.keystore -storepass example -alias example -keyalg rsa -validity 365 -keysize 2048

> keytool -certreq -keystore example.keystore -storepass example -alias example -keyalg rsa -file example.csr

> keytool -gencert -keystore example.keystore -storepass example -alias example -infile example.csr -outfile example.cert -validity 365

> keytool -import  -keystore example.keystore -storepass example -alias example -file example.cert

> keytool -import -file example.cert -keystore example.truststore -storepass example -alias example 


Adding SSL to an Existing Application
-----------------------------
If you already have a VoltDB application, enabling SSL is simple. 
- Prepare your key store and trust store. More information can be found in the "Preparation" section of this document.
- Add the following to your deployment.xml
```xml
    <ssl enabled="true" external="true">
        <keystore   path="example.keystore"   password="example"/>
        <truststore path="example.truststore" password="example"/>
    </ssl>
```
- Add the `--ssl=SSL.properties ` argument to `sqlcmd` to your script which loads the DDL.
- Add the following in your client:
```java
    clientConfig.setTrustStore(pathToTrustStore, trustStorePassword); // can also use setTrustStoreConfigFromPropertyFile();
    clientConfig.enableSSL();
```
