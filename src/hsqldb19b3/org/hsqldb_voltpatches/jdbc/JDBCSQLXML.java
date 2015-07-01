/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.xml.bind.util.JAXBResult;
import javax.xml.bind.util.JAXBSource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stax.StAXResult;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.lib.ClosableByteArrayOutputStream;
import org.hsqldb_voltpatches.lib.StringConverter;
import org.w3c.dom.DOMException;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentType;
import org.w3c.dom.Element;
import org.w3c.dom.EntityReference;
import org.w3c.dom.Node;
import org.w3c.dom.ProcessingInstruction;
import org.w3c.dom.Text;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/* $Id: JDBCSQLXML.java 2946 2009-03-22 17:44:48Z fredt $ */

/**
 * <!-- start generic documentation -->
 * The mapping in the JavaTM programming language for the SQL XML type.
 * XML is a built-in type that stores an XML value
 * as a column value in a row of a database table.
 * By default drivers implement an SQLXML object as
 * a logical pointer to the XML data
 * rather than the data itself.
 * An SQLXML object is valid for the duration of the transaction in which it was created.
 * <p>
 * The SQLXML interface provides methods for accessing the XML value
 * as a String, a Reader or Writer, or as a Stream.  The XML value
 * may also be accessed through a Source or set as a Result, which
 * are used with XML Parser APIs such as DOM, SAX, and StAX, as
 * well as with XSLT transforms and XPath evaluations.
 * <p>
 * Methods in the interfaces ResultSet, CallableStatement, and PreparedStatement,
 * such as getSQLXML allow a programmer to access an XML value.
 * In addition, this interface has methods for updating an XML value.
 * <p>
 * The XML value of the SQLXML instance may be obtained as a BinaryStream using
 * <pre>
 *   SQLXML sqlxml = resultSet.getSQLXML(column);
 *   InputStream binaryStream = sqlxml.getBinaryStream();
 * </pre>
 * For example, to parse an XML value with a DOM parser:
 * <pre>
 *   DocumentBuilder parser = DocumentBuilderFactory.newInstance().newDocumentBuilder();
 *   Document result = parser.parse(binaryStream);
 * </pre>
 * or to parse an XML value with a SAX parser to your handler:
 * <pre>
 *   SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
 *   parser.parse(binaryStream, myHandler);
 * </pre>
 * or to parse an XML value with a StAX parser:
 * <pre>
 *   XMLInputFactory factory = XMLInputFactory.newInstance();
 *   XMLStreamReader streamReader = factory.createXMLStreamReader(binaryStream);
 * </pre>
 * <p>
 * Because databases may use an optimized representation for the XML,
 * accessing the value through getSource() and
 * setResult() can lead to improved processing performance
 * without serializing to a stream representation and parsing the XML.
 * <p>
 * For example, to obtain a DOM Document Node:
 * <pre>
 *   DOMSource domSource = sqlxml.getSource(DOMSource.class);
 *   Document document = (Document) domSource.getNode();
 * </pre>
 * or to set the value to a DOM Document Node to myNode:
 * <pre>
 *   DOMResult domResult = sqlxml.setResult(DOMResult.class);
 *   domResult.setNode(myNode);
 * </pre>
 * or, to send SAX events to your handler:
 * <pre>
 *   SAXSource saxSource = sqlxml.getSource(SAXSource.class);
 *   XMLReader xmlReader = saxSource.getXMLReader();
 *   xmlReader.setContentHandler(myHandler);
 *   xmlReader.parse(saxSource.getInputSource());
 * </pre>
 * or, to set the result value from SAX events:
 * <pre>
 *   SAXResult saxResult = sqlxml.setResult(SAXResult.class);
 *   ContentHandler contentHandler = saxResult.getXMLReader().getContentHandler();
 *   contentHandler.startDocument();
 *   // set the XML elements and attributes into the result
 *   contentHandler.endDocument();
 * </pre>
 * or, to obtain StAX events:
 * <pre>
 *   StAXSource staxSource = sqlxml.getSource(StAXSource.class);
 *   XMLStreamReader streamReader = staxSource.getXMLStreamReader();
 * </pre>
 * or, to set the result value from StAX events:
 * <pre>
 *   StAXResult staxResult = sqlxml.getResult(StAXResult.class);
 *   XMLStreamWriter streamWriter = staxResult.getXMLStreamWriter();
 * </pre>
 * or, to perform XSLT transformations on the XML value using the XSLT in xsltFile
 * output to file resultFile:
 * <pre>
 *   File xsltFile = new File("a.xslt");
 *   File myFile = new File("result.xml");
 *   Transformer xslt = TransformerFactory.newInstance().newTransformer(new StreamSource(xsltFile));
 *   Source source = sqlxml.getSource(null);
 *   Result result = new StreamResult(myFile);
 *   xslt.transform(source, result);
 * </pre>
 * or, to evaluate an XPath expression on the XML value:
 * <pre>
 *   XPath xpath = XPathFactory.newInstance().newXPath();
 *   DOMSource domSource = sqlxml.getSource(DOMSource.class);
 *   Document document = (Document) domSource.getNode();
 *   String expression = "/foo/@bar";
 *   String barValue = xpath.evaluate(expression, document);
 * </pre>
 * To set the XML value to be the result of an XSLT transform:
 * <pre>
 *   File sourceFile = new File("source.xml");
 *   Transformer xslt = TransformerFactory.newInstance().newTransformer(new StreamSource(xsltFile));
 *   Source streamSource = new StreamSource(sourceFile);
 *   Result result = sqlxml.setResult(null);
 *   xslt.transform(streamSource, result);
 * </pre>
 * Any Source can be transformed to a Result using the identity transform
 * specified by calling newTransformer():
 * <pre>
 *   Transformer identity = TransformerFactory.newInstance().newTransformer();
 *   Source source = sqlxml.getSource(null);
 *   File myFile = new File("result.xml");
 *   Result result = new StreamResult(myFile);
 *   identity.transform(source, result);
 * </pre>
 * To write the contents of a Source to standard output:
 * <pre>
 *   Transformer identity = TransformerFactory.newInstance().newTransformer();
 *   Source source = sqlxml.getSource(null);
 *   Result result = new StreamResult(System.out);
 *   identity.transform(source, result);
 * </pre>
 * To create a DOMSource from a DOMResult:
 * <pre>
 *    DOMSource domSource = new DOMSource(domResult.getNode());
 * </pre>
 * <p>
 * Incomplete or invalid XML values may cause an SQLException when
 * set or the exception may occur when execute() occurs.  All streams
 * must be closed before execute() occurs or an SQLException will be thrown.
 * <p>
 * Reading and writing XML values to or from an SQLXML object can happen at most once.
 * The conceptual states of readable and not readable determine if one
 * of the reading APIs will return a value or throw an exception.
 * The conceptual states of writable and not writable determine if one
 * of the writing APIs will set a value or throw an exception.
 * <p>
 * The state moves from readable to not readable once free() or any of the
 * reading APIs are called: getBinaryStream(), getCharacterStream(), getSource(), and getString().
 * Implementations may also change the state to not writable when this occurs.
 * <p>
 * The state moves from writable to not writeable once free() or any of the
 * writing APIs are called: setBinaryStream(), setCharacterStream(), setResult(), and setString().
 * Implementations may also change the state to not readable when this occurs.
 * <p>
 * All methods on the <code>SQLXML</code> interface must be fully implemented if the
 * JDBC driver supports the data type.
 * <!-- end generic documentation -->
 *
 * <!-- start release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <h3>HSQLDB-Specific Information:</h3> <p>
 *
 * Starting with HSQLDB 1.9.0, a rudimentary client-side SQLXML interface
 * implementation (this class) is supported for local use when the product is
 * built and run under JDK 1.6+ and the SQLXML instance is constructed as the
 * result of calling JDBCConnection.createSQLXML(). <p>
 *
 * SQLXML instances retrieved in such a fashion are initially write-only, with
 * the lifecycle of read and write availability constrained in accordance with
 * the documentation of the interface methods. <p>
 *
 * When build and run under JDK 1.6+, it is also possible to retrieve read-only
 * SQLXML instances from JDBCResultSet.getSQLXML(...), given that the underlying
 * data can be converted to an XML Document Object Model (DOM). <p>
 *
 * However, at the time of this writing (2007-06-12) it is not yet possible to
 * store SQLXML objects directly into an HSQLDB database or to use them directly
 * for HSQLDB statement parameterization purposes.  This is because the SQLXML
 * data type is not yet natively supported by the HSQLDB engine. Instead, a
 * JDBCSQLXML instance must first be read as a string, binary input stream,
 * character input stream and so on, which can then be used for such purposes. <p>
 *
 * Here is the current read/write availability lifecycle for JDBCSQLXML:
 *
 * <TABLE border="1" cellspacing=1" cellpadding="3">
 *     <THEAD valign="bottom">
 *         <TR align="center">
 *             <TH>
 *                 Origin
 *             </TH>
 *             <TH>
 *                 Initially
 *             </TH>
 *             <TH>
 *                 After 1<SUP>st</SUP> Write
 *             </TH>
 *             <TH>
 *                 After 1<SUP>st</SUP> Read
 *             </TH>
 *             <TH>
 *                 After 1<SUP>st</SUP> Free
 *             </TH>
 *         </TR>
 *     </THEAD>
 *     <TBODY>
 *         <TR >
 *             <TH>
 *                 <tt>org.hsqldb_voltpatches.jdbc.JDBCConnection.createSQLXML()</tt>
 *             </TH>
 *             <TD >
 *                 Write-only
 *             </TD>
 *             <TD>
 *                 Read-only
 *             </TD>
 *             <TD>
 *                 Not readable or writable
 *             </TD>
 *             <TD>
 *                 Not readable or writable
 *             </TD>
 *         </TR>
 *         <TR>
 *             <TH>
 *                 <tt>org.hsqldb_voltpatches.jdbc.JDBCResultSet.getSQLXML(...)</tt>
 *             </TH>
 *             <TD >
 *                 Read-only
 *             </TD>
 *             <TD>
 *                 N/A
 *             </TD>
 *             <TD>
 *                 Not readable or writable
 *             </TD>
 *             <TD>
 *                 Not readable or writable
 *             </TD>
 *         </TR>
 *     </TBODY>
 * </TABLE>
 * </div>
 * <!-- end release-specific documentation -->
 *
 * @author boucherb@users
 * @see javax.xml.parsers
 * @see javax.xml.stream
 * @see javax.xml.transform
 * @see javax.xml.xpath
 * @since JDK 1.6, HSQLDB 1.9.0
 * @revised Mustang Build 79
 */
public class JDBCSQLXML implements SQLXML {

    private static String domFeatures = "XML 3.0 Traversal +Events 2.0";
    private static DOMImplementation         domImplementation;
    private static DOMImplementationRegistry domImplementationRegistry;
    private static ThreadPoolExecutor        executorService;
    private static Transformer               identityTransformer;
    private static TransformerFactory        transformerFactory;

    /**
     * Precomputed Charset to reduce octect to character sequence conversion
     * charset lookup overhead.
     */
    private static final Charset                utf8Charset;
    private static ArrayBlockingQueue<Runnable> workQueue;

    static {
        Charset charset = null;

        try {
            charset = Charset.forName("UTF8");
        } catch (Exception e) {
        }
        utf8Charset = charset;
    }

    /**
     * When non-null, the SAX ContentHandler currently in use to build this
     * object's SQLXML value from a SAX event sequence.
     */
    private SAX2DOMBuilder builder;

    /**
     * Whether this object is closed.  When closed, no further reading
     * or writing is possible.
     */
    private boolean closed;

    // ------------------------- Internal Implementation -----------------------

    /**
     * This object's SQLXML value as a GZIPed byte array
     */
    private volatile byte[] gzdata;

    /**
     * When non-null, the stream currently in use to read this object's
     * SQLXML value as an octet sequence.
     */
    private InputStream inputStream;

    /**
     * When non-null, the stream currently in use to write this object's
     * SQLXML value from an octet sequence.
     */
    private ClosableByteArrayOutputStream outputStream;

    /**
     * This object's public id
     */
    private String publicId;

    /**
     * Whether it is possible to read this object's SQLXML value.
     */
    private boolean readable;

    /**
     * This object's system id
     */
    private String systemId;

    /**
     * Whether it is possible to write this object's SQLXML value.
     */
    private boolean writable;

    /**
     * Constructs a new, initially write-only JDBCSQLXML object. <p>
     */
    protected JDBCSQLXML() {
        setReadable(false);
        setWritable(true);
    }

    /**
     * Constructs a new read-only JDBCSQLXML object from the given octet
     * sequence. <p>
     *
     * @param bytes the octet sequence representing the SQLXML value
     * @throws SQLException if the argument does not represent a
     *      valid SQLXML value
     */
    protected JDBCSQLXML(byte[] bytes) throws SQLException {
        this(bytes, null);
    }

    /**
     * Constructs a new read-only JDBCSQLXML object from the given character
     * sequence. <p>
     *
     * @param chars the character sequence representing the SQLXML value
     * @throws SQLException if the argument does not represent a
     *      valid SQLXML value
     */
    protected JDBCSQLXML(char[] chars) throws SQLException {
        this(chars, 0, chars.length, null);
    }

    /**
     * Constructs a new JDBCSQLXML object from the given Document. <p>
     *
     * @param document the Document representing the SQLXML value
     * @throws java.sql.SQLException if the argument does not represent a
     *      valid SQLXML value
     */
    protected JDBCSQLXML(Document document) throws SQLException {
        this(new DOMSource(document));
    }

    /**
     * Constructs a new read-only JDBCSQLXML object from the given octet
     * sequence. <p>
     *
     * Relative URI references will be resolved against the present working
     * directory reported by the Java virtual machine. <p>
     *
     * @param inputStream an octet stream representing an SQLXML value
     * @throws SQLException if the argument does not represent a
     *      valid SQLXML value
     */
    protected JDBCSQLXML(InputStream inputStream) throws SQLException {
        this(inputStream, null);
    }

    /**
     * Constructs a new read-only JDBCSQLXML object from the given character
     * sequence. <p>
     *
     * Relative URI references will be resolved against the present working
     * directory reported by the Java virtual machine. <p>
     *
     * <b>Note:</b>Normally, a byte stream should be used rather than a reader,
     * so that the XML parser can resolve character encoding specified by the
     * XML declaration. However, in many cases the encoding of the input stream
     * is already resolved, as in the case of reading XML from a StringReader.
     *
     * @param reader a character stream representing an SQLXML value
     * @throws SQLException if the argument does not represent a
     *      valid SQLXML value
     */
    protected JDBCSQLXML(Reader reader) throws SQLException {
        this(reader, null);
    }

    /**
     * Constructs a new read-only JDBCSQLXML object from the given Source
     * object. <p>
     *
     * @param source a Source representing an SQLXML value
     * @throws SQLException if the argument does not represent a
     *      valid SQLXML value
     */
    public JDBCSQLXML(Source source) throws SQLException {
        init(source);
    }

    /**
     * Constructs a new read-only JDBCSQLXML object from the given character
     * sequence. <p>
     *
     * Relative URI references will be resolved against the present working
     * directory reported by the Java virtual machine. <p>
     *
     * @param string a character sequence representing an SQLXML value
     * @throws SQLException if the argument does not represent a
     *      valid SQLXML value
     */
    protected JDBCSQLXML(String string) throws SQLException {
        this(new StreamSource(new StringReader(string)));
    }

    /**
     * Constructs a new read-only JDBCSQLXML object from the given octet
     * sequence. <p>
     *
     * @param bytes the octet sequence representing the SQLXML value
     * @param systemId must be a String that conforms to the URI syntax;
     *        allows relative URIs to be processed.
     * @throws SQLException if the argument does not represent a
     *      valid SQLXML value
     */
    protected JDBCSQLXML(byte[] bytes, String systemId) throws SQLException {
        this(new StreamSource(new ByteArrayInputStream(bytes), systemId));
    }

    protected JDBCSQLXML(char[] chars, String systemId) throws SQLException {
        this(chars, 0, chars.length, systemId);
    }

    /**
     * Constructs a new read-only JDBCSQLXML object from the given octet
     * sequence. <p>
     *
     * Relative URI references will be resolved against the given systemId. <p>
     *
     * @param inputStream an octet stream representing an SQLXML value
     * @param systemId a String that conforms to the URI syntax, indicating
     *      the URI from which the XML data is being read, so that relative URI
     *      references can be resolved
     * @throws SQLException if the argument does not represent a
     *      valid SQLXML value
     */
    protected JDBCSQLXML(InputStream inputStream,
                         String systemId) throws SQLException {
        this(new StreamSource(inputStream, systemId));
    }

    /**
     * Constructs a new read-only JDBCSQLXML object from the given character
     * sequence. <p>
     *
     * Relative URI references will be resolved against the given systemId. <p>
     *
     * <b>Note:</b>Normally, a byte stream should be used rather than a reader,
     * so that the XML parser can resolve character encoding specified by the
     * XML declaration. However, in many cases the encoding of the input stream
     * is already resolved, as in the case of reading XML from a StringReader.
     *
     * @param reader a character stream representing an SQLXML value;
     * @param systemId a String that conforms to the URI syntax, indicating
     *      the URI from which the XML data is being read, so that relative URI
     *      references can be resolved
     * @throws SQLException if the argument does not represent a
     *      valid SQLXML value
     */
    protected JDBCSQLXML(Reader reader, String systemId) throws SQLException {
        this(new StreamSource(reader, systemId));
    }

    /**
     * Constructs a new read-only JDBCSQLXML object from the given character
     * sequence. <p>
     *
     * Relative URI references will be resolved against the given systemId. <p>
     *
     * @param string a character sequence representing an SQLXML value
     * @param systemId a String that conforms to the URI syntax, indicating
     *      the URI from which the XML data is being read, so that relative URI
     *      references can be resolved
     * @throws SQLException if the argument does not represent a
     *      valid SQLXML value
     */
    protected JDBCSQLXML(String string, String systemId) throws SQLException {
        this(new StreamSource(new StringReader(string), systemId));
    }

    /**
     * Constructs a new read-only JDBCSQLXML object from the given gzipped octet
     * sequence. <p>
     *
     * @param bytes the gzipped octet sequence representing the SQLXML value
     * @param clone whether to clone the given gzipped octet sequence
     * @param systemId
     * @param publicId
     * @throws SQLException if the argument does not represent a
     *      valid SQLXML value
     */
    protected JDBCSQLXML(byte[] bytes, boolean clone, String systemId,
                         String publicId) throws SQLException {

        this.setGZipData(clone ? bytes.clone()
                               : bytes);

        this.systemId = systemId;
        this.publicId = publicId;
    }

    protected JDBCSQLXML(char[] chars, int offset, int length,
                         String systemId) throws SQLException {
        this(new StreamSource(new CharArrayReader(chars, offset, length),
                              systemId));
    }

    /**
     * This method closes this object and releases the resources that it held.
     * The SQL XML object becomes invalid and neither readable or writeable
     * when this method is called.
     *
     * After <code>free</code> has been called, any attempt to invoke a
     * method other than <code>free</code> will result in a <code>SQLException</code>
     * being thrown.  If <code>free</code> is called multiple times, the subsequent
     * calls to <code>free</code> are treated as a no-op.
     * @throws SQLException if there is an error freeing the XML value.
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6
     */
    public void free() throws SQLException {
        close();
    }

    /**
     * Retrieves the XML value designated by this SQLXML instance as a stream.
     * The bytes of the input stream are interpreted according to appendix F of the XML 1.0 specification.
     * The behavior of this method is the same as ResultSet.getBinaryStream()
     * when the designated column of the ResultSet has a type java.sql.Types of SQLXML.
     * <p>
     * The SQL XML object becomes not readable when this method is called and
     * may also become not writable depending on implementation.
     *
     * @return a stream containing the XML data.
     * @throws SQLException if there is an error processing the XML value.
     *   An exception is thrown if the state is not readable.
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6
     */
    public synchronized InputStream getBinaryStream() throws SQLException {

        checkClosed();
        checkReadable();

        InputStream inputStream = getBinaryStreamImpl();

        setReadable(false);
        setWritable(false);

        return inputStream;
    }

    /**
     * Retrieves a stream that can be used to write the XML value that this SQLXML instance represents.
     * The stream begins at position 0.
     * The bytes of the stream are interpreted according to appendix F of the XML 1.0 specification
     * The behavior of this method is the same as ResultSet.updateBinaryStream()
     * when the designated column of the ResultSet has a type java.sql.Types of SQLXML.
     * <p>
     * The SQL XML object becomes not writeable when this method is called and
     * may also become not readable depending on implementation.
     *
     * @return a stream to which data can be written.
     * @throws SQLException if there is an error processing the XML value.
     *   An exception is thrown if the state is not writable.
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6
     */
    public synchronized OutputStream setBinaryStream() throws SQLException {

        checkClosed();
        checkWritable();

        OutputStream outputStream = setBinaryStreamImpl();

        setWritable(false);
        setReadable(true);

        return outputStream;
    }

    /**
     * Retrieves the XML value designated by this SQLXML instance as a java.io.Reader object.
     * The format of this stream is defined by org.xml.sax.InputSource,
     * where the characters in the stream represent the unicode code points for
     * XML according to section 2 and appendix B of the XML 1.0 specification.
     * Although an encoding declaration other than unicode may be present,
     * the encoding of the stream is unicode.
     * The behavior of this method is the same as ResultSet.getCharacterStream()
     * when the designated column of the ResultSet has a type java.sql.Types of SQLXML.
     * <p>
     * The SQL XML object becomes not readable when this method is called and
     * may also become not writable depending on implementation.
     *
     * @return a stream containing the XML data.
     * @throws SQLException if there is an error processing the XML value.
     *   The getCause() method of the exception may provide a more detailed exception, for example,
     *   if the stream does not contain valid characters.
     *   An exception is thrown if the state is not readable.
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6
     */
    public synchronized Reader getCharacterStream() throws SQLException {

        checkClosed();
        checkReadable();

        Reader reader = getCharacterStreamImpl();

        setReadable(false);
        setWritable(false);

        return reader;
    }

    /**
     * Retrieves a stream to be used to write the XML value that this SQLXML instance represents.
     * The format of this stream is defined by org.xml.sax.InputSource,
     * where the characters in the stream represent the unicode code points for
     * XML according to section 2 and appendix B of the XML 1.0 specification.
     * Although an encoding declaration other than unicode may be present,
     * the encoding of the stream is unicode.
     * The behavior of this method is the same as ResultSet.updateCharacterStream()
     * when the designated column of the ResultSet has a type java.sql.Types of SQLXML.
     * <p>
     * The SQL XML object becomes not writeable when this method is called and
     * may also become not readable depending on implementation.
     *
     * @return a stream to which data can be written.
     * @throws SQLException if there is an error processing the XML value.
     *   The getCause() method of the exception may provide a more detailed exception, for example,
     *   if the stream does not contain valid characters.
     *   An exception is thrown if the state is not writable.
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6 Build 79
     */
    public synchronized Writer setCharacterStream() throws SQLException {

        checkClosed();
        checkWritable();

        Writer writer = setCharacterStreamImpl();

        setReadable(true);
        setWritable(false);

        return writer;
    }

    /**
     * Returns a string representation of the XML value designated by this SQLXML instance.
     * The format of this String is defined by org.xml.sax.InputSource,
     * where the characters in the stream represent the unicode code points for
     * XML according to section 2 and appendix B of the XML 1.0 specification.
     * Although an encoding declaration other than unicode may be present,
     * the encoding of the String is unicode.
     * The behavior of this method is the same as ResultSet.getString()
     * when the designated column of the ResultSet has a type java.sql.Types of SQLXML.
     * <p>
     * The SQL XML object becomes not readable when this method is called and
     * may also become not writable depending on implementation.
     *
     * @return a string representation of the XML value designated by this SQLXML instance.
     * @throws SQLException if there is an error processing the XML value.
     *   The getCause() method of the exception may provide a more detailed exception, for example,
     *   if the stream does not contain valid characters.
     *   An exception is thrown if the state is not readable.
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6
     */
    public synchronized String getString() throws SQLException {

        checkClosed();
        checkReadable();

        String value = getStringImpl();

        setReadable(false);
        setWritable(false);

        return value;
    }

    /**
     * Sets the XML value designated by this SQLXML instance to the given String representation.
     * The format of this String is defined by org.xml.sax.InputSource,
     * where the characters in the stream represent the unicode code points for
     * XML according to section 2 and appendix B of the XML 1.0 specification.
     * Although an encoding declaration other than unicode may be present,
     * the encoding of the String is unicode.
     * The behavior of this method is the same as ResultSet.updateString()
     * when the designated column of the ResultSet has a type java.sql.Types of SQLXML.
     * <p>
     * The SQL XML object becomes not writeable when this method is called and
     * may also become not readable depending on implementation.
     *
     * @param value the XML value
     * @throws SQLException if there is an error processing the XML value.
     *   The getCause() method of the exception may provide a more detailed exception, for example,
     *   if the stream does not contain valid characters.
     *   An exception is thrown if the state is not writable.
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6
     */
    public synchronized void setString(String value) throws SQLException {

        if (value == null) {
            throw Util.nullArgument("value");
        }
        checkWritable();
        setStringImpl(value);
        setReadable(true);
        setWritable(false);
    }

    /**
     * Returns a Source for reading the XML value designated by this SQLXML instance.
     * Sources are used as inputs to XML parsers and XSLT transformers.
     * <p>
     * Sources for XML parsers will have namespace processing on by default.
     * The systemID of the Source is implementation dependent.
     * <p>
     * The SQL XML object becomes not readable when this method is called and
     * may also become not writable depending on implementation.
     * <p>
     * Note that SAX is a callback architecture, so a returned
     * SAXSource should then be set with a content handler that will
     * receive the SAX events from parsing.  The content handler
     * will receive callbacks based on the contents of the XML.
     * <pre>
     *   SAXSource saxSource = sqlxml.getSource(SAXSource.class);
     *   XMLReader xmlReader = saxSource.getXMLReader();
     *   xmlReader.setContentHandler(myHandler);
     *   xmlReader.parse(saxSource.getInputSource());
     * </pre>
     *
     * @param sourceClass The class of the source, or null.
     * If the class is null, a vendor specifc Source implementation will be returned.
     * The following classes are supported at a minimum:
     * <pre>
     *   javax.xml.transform.dom.DOMSource - returns a DOMSource
     *   javax.xml.transform.sax.SAXSource - returns a SAXSource
     *   javax.xml.transform.stax.StAXSource - returns a StAXSource
     *   javax.xml.transform.stream.StreamSource - returns a StreamSource
     * </pre>
     * @return a Source for reading the XML value.
     * @throws SQLException if there is an error processing the XML value
     *   or if this feature is not supported.
     *   The getCause() method of the exception may provide a more detailed exception, for example,
     *   if an XML parser exception occurs.
     *   An exception is thrown if the state is not readable.
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6 Build 79
     */
    @SuppressWarnings("unchecked")
    public synchronized <T extends Source>T getSource(
            Class<T> sourceClass) throws SQLException {

        checkClosed();
        checkReadable();

        final Source source = getSourceImpl(sourceClass);

        setReadable(false);
        setWritable(false);

        return (T) source;
    }

    /**
     * Returns a Result for setting the XML value designated by this SQLXML instance.
     * <p>
     * The systemID of the Result is implementation dependent.
     * <p>
     * The SQL XML object becomes not writeable when this method is called and
     * may also become not readable depending on implementation.
     * <p>
     * Note that SAX is a callback architecture and the returned
     * SAXResult has a content handler assigned that will receive the
     * SAX events based on the contents of the XML.  Call the content
     * handler with the contents of the XML document to assign the values.
     * <pre>
     *   SAXResult saxResult = sqlxml.getResult(SAXResult.class);
     *   ContentHandler contentHandler = saxResult.getXMLReader().getContentHandler();
     *   contentHandler.startDocument();
     *   // set the XML elements and attributes into the result
     *   contentHandler.endDocument();
     * </pre>
     *
     * @param resultClass The class of the result, or null.
     * If resultClass is null, a vendor specific Result implementation will be returned.
     * The following classes are supported at a minimum:
     * <pre>
     *   javax.xml.transform.dom.DOMResult - returns a DOMResult
     *   javax.xml.transform.sax.SAXResult - returns a SAXResult
     *   javax.xml.transform.stax.StAXResult - returns a StAXResult
     *   javax.xml.transform.stream.StreamResult - returns a StreamResult
     * </pre>
     * @return Returns a Result for setting the XML value.
     * @throws SQLException if there is an error processing the XML value
     *   or if this feature is not supported.
     *   The getCause() method of the exception may provide a more detailed exception, for example,
     *   if an XML parser exception occurs.
     *   An exception is thrown if the state is not writable.
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6 Build 79
     */
    public synchronized <T extends Result>T setResult(
            Class<T> resultClass) throws SQLException {

        checkClosed();
        checkWritable();

        final T result = createResult(resultClass);

        setReadable(true);
        setWritable(false);

        return result;
    }

    /**
     * @return that may be used to perform processesing asynchronously.
     */
    protected static ExecutorService getExecutorService() {

        if (JDBCSQLXML.executorService == null) {
            int      corePoolSize    = 1;
            int      maximumPoolSize = 10;
            long     keepAliveTime   = 1;
            TimeUnit unit            = TimeUnit.SECONDS;

            JDBCSQLXML.workQueue = new ArrayBlockingQueue<Runnable>(10);
            JDBCSQLXML.executorService = new ThreadPoolExecutor(corePoolSize,
                    maximumPoolSize, keepAliveTime, unit, workQueue);
        }

        return executorService;
    }

    /**
     * @return with which to obtain xml transformer instances.
     * @throws java.sql.SQLException when unable to obtain a factory instance.
     */
    protected static TransformerFactory getTransformerFactory() throws SQLException {

        if (JDBCSQLXML.transformerFactory == null) {
            try {
                JDBCSQLXML.transformerFactory =
                    TransformerFactory.newInstance();
            } catch (TransformerFactoryConfigurationError ex) {
                throw Exceptions.transformFailed(ex);
            }
        }

        return JDBCSQLXML.transformerFactory;
    }

    /**
     * @return used to perform identity transforms
     * @throws java.sql.SQLException when unable to obtain the instance.
     */
    protected static Transformer getIdentityTransformer() throws SQLException {

        if (JDBCSQLXML.identityTransformer == null) {
            try {
                JDBCSQLXML.identityTransformer =
                    getTransformerFactory().newTransformer();
            } catch (TransformerConfigurationException ex) {
                throw Exceptions.transformFailed(ex);
            }
        }

        return JDBCSQLXML.identityTransformer;
    }

    /**
     * @return with which to construct DOM implementation instances.
     * @throws java.sql.SQLException when unable to obtain a factory instance.
     */
    protected static DOMImplementationRegistry getDOMImplementationRegistry() throws SQLException {

        if (domImplementationRegistry == null) {
            try {
                domImplementationRegistry =
                    DOMImplementationRegistry.newInstance();
            } catch (ClassCastException ex) {
                throw Exceptions.domInstantiation(ex);
            } catch (InstantiationException ex) {
                throw Exceptions.domInstantiation(ex);
            } catch (ClassNotFoundException ex) {
                throw Exceptions.domInstantiation(ex);
            } catch (IllegalAccessException ex) {
                throw Exceptions.domInstantiation(ex);
            }
        }

        return domImplementationRegistry;
    }

    /**
     * @return with which to create document instances.
     * @throws java.sql.SQLException when unable to obtain the DOM
     *         implementation instance.
     */
    protected static DOMImplementation getDOMImplementation() throws SQLException {

        if (domImplementation == null) {
            domImplementation =
                getDOMImplementationRegistry().getDOMImplementation(
                    domFeatures);
        }

        if (domImplementation == null) {
            Exception ex = new RuntimeException("Not supported: "
                + domFeatures);

            throw Exceptions.domInstantiation(ex);
        }

        return domImplementation;
    }

    /**
     * @param namespaceURI of the document element to create or <code>null</code>.
     * @param qualifiedName of the document element to be created or <code>null</code>.
     * @param docType of document to be created or <code>null</code>.
     *   When <code>doctype</code> is not <code>null</code>, its
     *   <code>Node.ownerDocument</code> attribute is set to the document
     *   being created.
     * @return with its document element.
     *   If the <code>NamespaceURI</code>, <code>qualifiedName</code>, and
     *   <code>doctype</code> are <code>null</code>, the returned
     *   <code>Document</code> is empty with no document element.
     * @throws java.sql.SQLException wrapping any internal exception that occurs.
     * @see org.w3c.dom.DOMImplementation#createDocument(String,String,DocumentType)
     */
    protected static Document createDocument(String namespaceURI,
            String qualifiedName, DocumentType docType) throws SQLException {

        try {
            return getDOMImplementation().createDocument(namespaceURI,
                    qualifiedName, docType);
        } catch (DOMException ex) {
            throw Exceptions.domInstantiation(ex);
        }
    }

    /**
     * @return that is empty with no document element.
     * @throws java.sql.SQLException wrapping any internal exception that occurs.
     */
    protected static Document createDocument() throws SQLException {
        return createDocument(null, null, null);
    }

    /**
     * Initializes this object's SQLXML value from the given Source
     * object. <p>
     *
     * @param source the Source representing the SQLXML value
     * @throws SQLException if the argument does not represent a
     *      valid SQLXML value
     */
    protected void init(Source source) throws SQLException {

        if (source == null) {
            throw Util.nullArgument("source");
        }

        Transformer           transformer =
            JDBCSQLXML.getIdentityTransformer();
        StreamResult          result      = new StreamResult();
        ByteArrayOutputStream baos        = new ByteArrayOutputStream();
        GZIPOutputStream      gzos;

        try {
            gzos = new GZIPOutputStream(baos);
        } catch (IOException ex) {
            throw Exceptions.transformFailed(ex);
        }
        result.setOutputStream(gzos);

        try {
            transformer.transform(source, result);
        } catch (TransformerException ex) {
            throw Exceptions.transformFailed(ex);
        }

        try {
            gzos.close();
        } catch (IOException ex) {
            throw Exceptions.transformFailed(ex);
        }

        byte[] data = baos.toByteArray();

        setGZipData(data);
        setReadable(true);
        setWritable(false);
    }

    /**
     * Assigns this object's SQLXML value from the designated gzipped array
     * of bytes.
     *
     * @param data the SQLXML value
     * @throws java.sql.SQLException if the argument does not represent a
     *      valid SQLXML value
     */
    protected void setGZipData(byte[] data) throws SQLException {

        if (data == null) {
            throw Util.nullArgument("data");
        }
        this.gzdata = data;
    }

    /**
     * Directly retrieves this object's present SQLMXL value as a gzipped
     * array of bytes. <p>
     *
     * May be null, empty or invalid.
     *
     * @return this object's SQLMXL value as a gzipped byte array
     */
    protected byte[] gZipData() {
        return this.gzdata;
    }

    /**
     * Retrieves this object's SQLXML value as a gzipped array of bytes,
     * possibly by terminating any in-progress write operations and converting
     * accumulated intermediate data.
     *
     * @throws java.sql.SQLException if an underlying I/O or transform
     *      error occurs
     * @return this object's SQLXML value
     */
    protected byte[] getGZipData() throws SQLException {

        byte[] bytes = gZipData();

        if (bytes != null) {
            return bytes;
        }

        if ((this.outputStream == null) || !this.outputStream.isClosed()
                || this.outputStream.isFreed()) {
            throw Exceptions.notReadable();
        }

        try {
            setGZipData(this.outputStream.toByteArray());

            return gZipData();
        } catch (IOException ex) {
            throw Exceptions.notReadable();
        } finally {
            this.freeOutputStream();
        }
    }

    /**
     * closes this object and releases the resources that it holds.
     */
    protected synchronized void close() {

        this.closed = true;

        setReadable(false);
        setWritable(false);
        freeOutputStream();
        freeInputStream();

        this.gzdata = null;
    }

    /**
     * Closes the input stream, if any, currently in use to read this object's
     * SQLXML value and nullifies the stream reference to make it elligible
     * for subsequent garbage collection.
     */
    protected void freeInputStream() {

        if (this.inputStream != null) {
            try {
                this.inputStream.close();
            } catch (IOException ex) {

                // ex.printStackTrace();
            } finally {
                this.inputStream = null;
            }
        }
    }

    /**
     * Closes the output stream, if any, currently in use to write this object's
     * SQLXML value and nullifies the stream reference to make it elligible for
     * subsequent garbage collection.  The stream's data buffer reference may
     * also be nullified, in order to make it elligible for garbage collection
     * immediately, just in case an external client still holds a reference to
     * the output stream.
     */
    protected void freeOutputStream() {

        if (this.outputStream != null) {
            try {
                this.outputStream.free();
            } catch (IOException ex) {

                // ex.printStackTrace();
            }
            this.outputStream = null;
        }
    }

    /**
     * Checks whether this object is closed (has been freed).
     *
     * @throws java.sql.SQLException if this object is closed.
     */
    protected synchronized void checkClosed() throws SQLException {

        if (this.closed) {
            throw Exceptions.inFreedState();
        }
    }

    /**
     * Checks whether this object is readable.
     *
     * @throws java.sql.SQLException if this object is not readable.
     */
    protected synchronized void checkReadable() throws SQLException {

        if (!this.isReadable()) {
            throw Exceptions.notReadable();
        }
    }

    /**
     * Assigns this object's readability status.
     *
     * @param readable if <tt>true</tt>, then readable; else not readable
     */
    protected synchronized void setReadable(boolean readable) {
        this.readable = readable;
    }

    /**
     * Checks whether this object is writable.
     *
     * @throws java.sql.SQLException if this object is not writable.
     */
    protected synchronized void checkWritable() throws SQLException {

        if (!this.isWritable()) {
            throw Exceptions.notWritable();
        }
    }

    /**
     * Assigns this object's writability status.
     *
     * @param writable if <tt>true</tt>, then writable; else not writable
     */
    protected synchronized void setWritable(boolean writable) {
        this.writable = writable;
    }

    /**
     * Retrieves the object's readability status.
     *
     * @return if <tt>true</tt>, then readable; else not readable
     */
    public synchronized boolean isReadable() {
        return this.readable;
    }

    /**
     * Retrieves the object's readability status.
     *
     * @return if <tt>true</tt>, then writable; else not writable
     */
    public synchronized boolean isWritable() {
        return this.writable;
    }

    /**
     * Retrieves a stream representing the XML value designated by this
     * SQLXML instance. <p>
     *
     * @return a stream containing the XML data.
     * @throws SQLException if there is an error processing the XML value.
     */
    protected InputStream getBinaryStreamImpl() throws SQLException {

        try {
            return new GZIPInputStream(
                new ByteArrayInputStream(getGZipData()));
        } catch (IOException ex) {
            throw Exceptions.transformFailed(ex);
        }
    }

    /**
     * Retrieves a reader representing the XML value designated by this
     * SQLXML instance. <p>
     *
     * @return a reader containing the XML data.
     * @throws SQLException if there is an error processing the XML value.
     */
    protected Reader getCharacterStreamImpl() throws SQLException {
        return new InputStreamReader(getBinaryStreamImpl());
    }

    /**
     * Retrieves a string representing the XML value designated by this
     * SQLXML instance. <p>
     *
     * @return a string containing the XML data.
     * @throws SQLException if there is an error processing the XML value.
     */
    protected String getStringImpl() throws SQLException {

        try {
            return StringConverter.inputStreamToString(getBinaryStreamImpl(),
                    "US-ASCII");
        } catch (IOException ex) {
            throw Exceptions.transformFailed(ex);
        }
    }

    /**
     * Retrieves a stream to completely write the XML value this SQLXML
     * instance represents. <p>
     *
     * @return a stream to which the data can be written.
     * @throws SQLException if there is an error processing the XML value.
     */
    protected OutputStream setBinaryStreamImpl() throws SQLException {

        this.outputStream = new ClosableByteArrayOutputStream();

        try {
            return new GZIPOutputStream(this.outputStream);
        } catch (IOException ex) {
            this.outputStream = null;

            throw Exceptions.resultInstantiation(ex);
        }
    }

    /**
     * Retrieves a writer to completely write the XML value this SQLXML
     * instance represents. <p>
     *
     * @return a writer to which the data can be written.
     * @throws SQLException if there is an error processing the XML value.
     *   The getCause() method of the exception may provide a more detailed exception, for example,
     *   if the stream does not contain valid characters.
     *   An exception is thrown if the state is not writable.
     * @since JDK 1.6 Build 79
     */
    protected Writer setCharacterStreamImpl() throws SQLException {
        return new OutputStreamWriter(setBinaryStreamImpl());
    }

    /**
     * Sets the XML value designated by this SQLXML instance using the given
     * String representation. <p>
     *
     * @param value the XML value
     * @throws SQLException if there is an error processing the XML value.
     */
    protected void setStringImpl(String value) throws SQLException {
        init(new StreamSource(new StringReader(value)));
    }

    /**
     * Returns a Source for reading the XML value designated by this SQLXML
     * instance. <p>
     *
     * @param sourceClass The class of the source, or null.  If null, then a
     *      DOMSource is returned.
     * @return a Source for reading the XML value.
     * @throws SQLException if there is an error processing the XML value
     *   or if the given <tt>sourceClass</tt> is not supported.
     */
    protected <T extends Source>T getSourceImpl(
            Class<T> sourceClass) throws SQLException {

        if (JAXBSource.class.isAssignableFrom(sourceClass)) {

            // Must go first presently, since JAXBSource extends SAXSource
            // (purely as an implmentation detail) and it's not possible
            // to instantiate a valid JAXBSource with a Zero-Args
            // constructor(or any subclass thereof, due to the finality of
            // its private marshaller and context object attrbutes)
            // FALL THROUGH... will throw an exception
        } else if (StreamSource.class.isAssignableFrom(sourceClass)) {
            return createStreamSource(sourceClass);
        } else if ((sourceClass == null)
                   || DOMSource.class.isAssignableFrom(sourceClass)) {
            return createDOMSource(sourceClass);
        } else if (SAXSource.class.isAssignableFrom(sourceClass)) {
            return createSAXSource(sourceClass);
        } else if (StAXSource.class.isAssignableFrom(sourceClass)) {
            return createStAXSource(sourceClass);
        }

        throw Util.invalidArgument("sourceClass: " + sourceClass);
    }

    /**
     * Retrieves a new StreamSource for reading the XML value designated by this
     * SQLXML instance. <p>
     *
     * @param sourceClass The class of the source
     * @throws java.sql.SQLException if there is an error processing the XML
     *      value or if the given <tt>sourceClass</tt> is not supported.
     * @return a new StreamSource for reading the XML value designated by this
     *      SQLXML instance
     */
    @SuppressWarnings("unchecked")
    protected <T extends Source>T createStreamSource(
            Class<T> sourceClass) throws SQLException {

        StreamSource source = null;

        try {
            source = (sourceClass == null) ? new StreamSource()
                    : (StreamSource) sourceClass.newInstance();
        } catch (SecurityException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (InstantiationException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (IllegalAccessException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (ClassCastException ex) {
            throw Exceptions.sourceInstantiation(ex);
        }

        Reader reader = getCharacterStreamImpl();

        source.setReader(reader);

        return (T) source;
    }

    /**
     * Retrieves a new DOMSource for reading the XML value designated by this
     * SQLXML instance. <p>
     *
     * @param sourceClass The class of the source
     * @throws java.sql.SQLException if there is an error processing the XML
     *      value or if the given <tt>sourceClass</tt> is not supported.
     * @return a new DOMSource for reading the XML value designated by this
     *      SQLXML instance
     */
    @SuppressWarnings("unchecked")
    protected <T extends Source>T createDOMSource(
            Class<T> sourceClass) throws SQLException {

        DOMSource source = null;

        try {
            source = (sourceClass == null) ? new DOMSource()
                    : (DOMSource) sourceClass.newInstance();
        } catch (SecurityException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (IllegalAccessException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (InstantiationException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (ClassCastException ex) {
            throw Exceptions.sourceInstantiation(ex);
        }

        Transformer  transformer  = JDBCSQLXML.getIdentityTransformer();
        InputStream  inputStream  = this.getBinaryStreamImpl();
        StreamSource streamSource = new StreamSource();
        DOMResult    domResult    = new DOMResult();

        streamSource.setInputStream(inputStream);

        try {
            transformer.transform(streamSource, domResult);
        } catch (TransformerException ex) {
            throw Exceptions.transformFailed(ex);
        }
        source.setNode(domResult.getNode());

        return (T) source;
    }

    /**
     * Retrieves a new SAXSource for reading the XML value designated by this
     * SQLXML instance. <p>
     *
     * @param sourceClass The class of the source
     * @throws java.sql.SQLException if there is an error processing the XML
     *      value or if the given <tt>sourceClass</tt> is not supported.
     * @return a new SAXSource for reading the XML value designated by this
     *      SQLXML instance
     */
    @SuppressWarnings("unchecked")
    protected <T extends Source>T createSAXSource(
            Class<T> sourceClass) throws SQLException {

        SAXSource source = null;

        try {
            source = (sourceClass == null) ? new SAXSource()
                    : (SAXSource) sourceClass.newInstance();
        } catch (SecurityException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (InstantiationException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (IllegalAccessException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (ClassCastException ex) {
            throw Exceptions.sourceInstantiation(ex);
        }

        Reader      reader      = getCharacterStreamImpl();
        InputSource inputSource = new InputSource(reader);

        source.setInputSource(inputSource);

        return (T) source;
    }

    /**
     * Retrieves a new StAXSource for reading the XML value designated by this
     * SQLXML instance. <p>
     *
     * @param sourceClass The class of the source
     * @throws java.sql.SQLException if there is an error processing the XML
     *      value or if the given <tt>sourceClass</tt> is not supported.
     * @return a new StAXSource for reading the XML value designated by this
     *      SQLXML instance
     */
    @SuppressWarnings("unchecked")
    protected <T extends Source>T createStAXSource(
            Class<T> sourceClass) throws SQLException {

        StAXSource      source      = null;
        Constructor     sourceCtor  = null;
        Reader          reader      = null;
        XMLInputFactory factory     = null;
        XMLEventReader  eventReader = null;

        try {
            factory = XMLInputFactory.newInstance();
        } catch (FactoryConfigurationError ex) {
            throw Exceptions.sourceInstantiation(ex);
        }

        try {
            sourceCtor =
                (sourceClass == null)
                ? StAXSource.class.getConstructor(XMLEventReader.class)
                : sourceClass.getConstructor(XMLEventReader.class);
        } catch (SecurityException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (NoSuchMethodException ex) {
            throw Exceptions.sourceInstantiation(ex);
        }
        reader = getCharacterStreamImpl();

        try {
            eventReader = factory.createXMLEventReader(reader);
        } catch (XMLStreamException ex) {
            throw Exceptions.sourceInstantiation(ex);
        }

        try {
            source = (StAXSource) sourceCtor.newInstance(eventReader);
        } catch (SecurityException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (IllegalArgumentException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (IllegalAccessException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (InstantiationException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (InvocationTargetException ex) {
            throw Exceptions.sourceInstantiation(ex.getTargetException());
        } catch (ClassCastException ex) {
            throw Exceptions.sourceInstantiation(ex);
        }

        return (T) source;
    }

    /**
     * Retrieves a new Result for setting the XML value designated by this
     * SQLXML instance.
     *
     * @param resultClass The class of the result, or null.
     * @throws java.sql.SQLException if there is an error processing the XML
     *         value or the state is not writable
     * @return for setting the XML value designated by this SQLXML instance.
     */
    protected <T extends Result>T createResult(
            Class<T> resultClass) throws SQLException {

        checkWritable();
        setWritable(false);
        setReadable(true);

        if (JAXBResult.class.isAssignableFrom(resultClass)) {

            // Must go first presently, since JAXBResult extends SAXResult
            // (purely as an implmentation detail) and it's not possible
            // to instantiate a valid JAXBResult with a Zero-Args
            // constructor(or any subclass thereof, due to the finality of
            // its private UnmarshallerHandler)
            // FALL THROUGH... will throw an exception
        } else if ((resultClass == null)
                   || StreamResult.class.isAssignableFrom(resultClass)) {
            return createStreamResult(resultClass);
        } else if (DOMResult.class.isAssignableFrom(resultClass)) {
            return createDOMResult(resultClass);
        } else if (SAXResult.class.isAssignableFrom(resultClass)) {
            return createSAXResult(resultClass);
        } else if (StAXResult.class.isAssignableFrom(resultClass)) {
            return createStAXResult(resultClass);
        }

        throw Util.invalidArgument("resultClass: " + resultClass);
    }

    /**
     * Retrieves a new StreamResult for setting the XML value designated by this
     * SQLXML instance.
     *
     * @param resultClass The class of the result, or null.
     * @throws java.sql.SQLException if there is an error processing the XML
     *         value
     * @return for setting the XML value designated by this SQLXML instance.
     */

//  @SuppressWarnings("unchecked")
    protected <T extends Result>T createStreamResult(
            Class<T> resultClass) throws SQLException {

        StreamResult result = null;

        try {
            result = (resultClass == null) ? new StreamResult()
                    : (StreamResult) resultClass.newInstance();
        } catch (SecurityException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (InstantiationException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (IllegalAccessException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (ClassCastException ex) {
            throw Exceptions.resultInstantiation(ex);
        }

        OutputStream outputStream = setBinaryStreamImpl();

        result.setOutputStream(outputStream);

        return (T) result;
    }

    /**
     * Retrieves a new DOMResult for setting the XML value designated by this
     * SQLXML instance.
     *
     * @param resultClass The class of the result, or null.
     * @throws java.sql.SQLException if there is an error processing the XML
     *         value
     * @return for setting the XML value designated by this SQLXML instance.
     */
    @SuppressWarnings("unchecked")
    protected <T extends Result>T createDOMResult(
            Class<T> resultClass) throws SQLException {

        try {
            return (resultClass == null) ? ((T) new DOMResult())
                    : resultClass.newInstance();
        } catch (SecurityException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (InstantiationException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (IllegalAccessException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (ClassCastException ex) {
            throw Exceptions.resultInstantiation(ex);
        }
    }

    /**
     *  Retrieves a new SAXResult for setting the XML value designated by this
     *  SQLXML instance.
     *
     *  @param resultClass The class of the result, or null.
     *  @throws java.sql.SQLException if there is an error processing the XML
     *          value
     *  @return for setting the XML value designated by this SQLXML instance.
     */
    @SuppressWarnings("unchecked")
    protected <T extends Result>T createSAXResult(
            Class<T> resultClass) throws SQLException {

        SAXResult result = null;

        try {
            result = (resultClass == null) ? new SAXResult()
                    : (SAXResult) resultClass.newInstance();
        } catch (SecurityException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (InstantiationException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (IllegalAccessException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (ClassCastException ex) {
            throw Exceptions.resultInstantiation(ex);
        }

        StAXResult          staxResult = createStAXResult(null);
        XMLStreamWriter     xmlWriter  = staxResult.getXMLStreamWriter();
        SAX2XMLStreamWriter handler    = new SAX2XMLStreamWriter(xmlWriter);

        result.setHandler(handler);

        return (T) result;
    }

    /**
     *  Retrieves a new DOMResult for setting the XML value designated by this
     *  SQLXML instance.
     *
     *  @param resultClass The class of the result, or null.
     *  @throws java.sql.SQLException if there is an error processing the XML
     *          value
     *  @return for setting the XML value designated by this SQLXML instance.
     */
    @SuppressWarnings("unchecked")
    protected <T extends Result>T createStAXResult(
            Class<T> resultClass) throws SQLException {

        StAXResult       result       = null;
        OutputStream     outputStream = this.setBinaryStreamImpl();
        Constructor      ctor;
        XMLOutputFactory factory;
        XMLStreamWriter  xmlStreamWriter;

        try {
            factory         = XMLOutputFactory.newInstance();
            xmlStreamWriter = factory.createXMLStreamWriter(outputStream);

            if (resultClass == null) {
                result = new StAXResult(xmlStreamWriter);
            } else {
                ctor   = resultClass.getConstructor(XMLStreamWriter.class);
                result = (StAXResult) ctor.newInstance(xmlStreamWriter);
            }
        } catch (SecurityException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (IllegalArgumentException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (IllegalAccessException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (InvocationTargetException ex) {
            throw Exceptions.resultInstantiation(ex.getTargetException());
        } catch (FactoryConfigurationError ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (InstantiationException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (NoSuchMethodException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (XMLStreamException ex) {
            throw Exceptions.resultInstantiation(ex);
        }

        return (T) result;
    }

    /**
     * Basically just a namespace to isolate SQLXML exception generation
     */
    protected static class Exceptions {

        /**
         * Construction Disabled.
         */
        private Exceptions() {
        }

        /**
         *  Retrieves a new SQLXML DOM instantiation exception.
         *
         * @param cause of the exception
         */
        static SQLException domInstantiation(Throwable cause) {

            SQLException ex = Util.sqlException(ErrorCode.GENERAL_ERROR,
                "SQLXML DOM instantiation failed: " + cause);

            ex.initCause(cause);

            return ex;
        }

        /**
         * Retrieves a new SQLXML source instantiation exception.
         *
         * @param cause of the exception.
         * @return a new SQLXML source instantiation exception
         */
        static SQLException sourceInstantiation(Throwable cause) {

            SQLException ex = Util.sqlException(ErrorCode.GENERAL_ERROR,
                "SQLXML Source instantiation failed: " + cause);

            ex.initCause(cause);

            return ex;
        }

        /**
         * Retrieves a new SQLXML result instantiation exception.
         *
         * @param cause of the exception.
         * @return a new SQLXML result instantiation exception
         */
        static SQLException resultInstantiation(Throwable cause) {

            SQLException ex = Util.sqlException(ErrorCode.GENERAL_ERROR,
                "SQLXML Result instantiation failed: " + cause);

            ex.initCause(cause);

            return ex;
        }

        /**
         * Retrieves a new SQLXML parse failed exception.
         *
         * @param cause of the exception.
         * @return a new SQLXML parse failed exception
         */
        static SQLException parseFailed(Throwable cause) {

            SQLException ex = Util.sqlException(ErrorCode.GENERAL_IO_ERROR,
                "parse failed: " + cause);

            ex.initCause(cause);

            return ex;
        }

        /**
         * Retrieves a new SQLXML transform failed exception.
         *
         * @param cause of the exception.
         * @return a new SQLXML parse failed exception
         */
        static SQLException transformFailed(Throwable cause) {

            SQLException ex = Util.sqlException(ErrorCode.GENERAL_IO_ERROR,
                "transform failed: " + cause);

            ex.initCause(cause);

            return ex;
        }

        /**
         * Retrieves a new SQLXML not readable exception.
         *
         * @return a new SQLXML not readable exception
         */
        static SQLException notReadable() {
            return Util.sqlException(ErrorCode.GENERAL_IO_ERROR,
                                     "SQLXML in not readable state");
        }

        /**
         * Retrieves a new SQLXML not writable exception.
         *
         * @return a new SQLXML not writable exception
         */
        static SQLException notWritable() {
            return Util.sqlException(ErrorCode.GENERAL_IO_ERROR,
                                     "SQLXML in not writable state");
        }

        /**
         * Currently unused.
         *
         * @return never
         */
        static SQLException directUpdateByLocatorNotSupported() {
            return Util.sqlException(ErrorCode.X_0A000,
                                     "SQLXML direct update by locator");
        }

        /**
         * Retrieves a new SQLXML in freed state exception.
         *
         * @return a new SQLXML in freed state exception
         */
        static SQLException inFreedState() {
            return Util.sqlException(ErrorCode.GENERAL_ERROR,
                                     "SQLXML in freed state");
        }
    }

    // -------------------------------------------------------------------------

    /**
     * Builds a DOM from SAX events.
     */
    protected static class SAX2DOMBuilder implements ContentHandler,
            Closeable {

        /**
         *
         */
        private boolean closed;

        /**
         *
         */
        private Element currentElement;

        // --------------------- internal implementation -----------------------

        /**
         *
         */
        private Node currentNode;

        /**
         *
         */
        private Document document;

        /**
         *
         */
        private Locator locator;

        /**
         * <p>Creates a new instance of SAX2DOMBuilder, which creates
         * a new document. The document is available via
         * {@link #getDocument()}.</p>
         * @throws javax.xml.parsers.ParserConfigurationException
         */
        public SAX2DOMBuilder() throws ParserConfigurationException {

            DocumentBuilderFactory documentBuilderFactory;
            DocumentBuilder        documentBuilder;

            documentBuilderFactory = DocumentBuilderFactory.newInstance();

            documentBuilderFactory.setValidating(false);
            documentBuilderFactory.setNamespaceAware(true);

            documentBuilder  = documentBuilderFactory.newDocumentBuilder();
            this.document    = documentBuilder.newDocument();
            this.currentNode = this.document;
        }

        /**
         * Receive an object for locating the origin of SAX document events.
         *
         * <p>SAX parsers are strongly encouraged (though not absolutely
         * required) to supply a locator: if it does so, it must supply
         * the locator to the application by invoking this method before
         * invoking any of the other methods in the ContentHandler
         * interface.</p>
         *
         * <p>The locator allows the application to determine the end
         * position of any document-related event, even if the parser is
         * not reporting an error.  Typically, the application will
         * use this information for reporting its own errors (such as
         * character content that does not match an application's
         * business rules).  The information returned by the locator
         * is probably not sufficient for use with a search engine.</p>
         *
         * <p>Note that the locator will return correct information only
         * during the invocation SAX event callbacks after
         * {@link #startDocument startDocument} returns and before
         * {@link #endDocument endDocument} is called.  The
         * application should not attempt to use it at any other time.</p>
         *
         * @param locator an object that can return the location of
         *                any SAX document event
         * @see org.xml.sax.Locator
         */
        public void setDocumentLocator(Locator locator) {
            this.locator = locator;
        }

        /**
         * Retrieves the Locator. <p>
         * @return the Locator
         */
        public Locator getDocumentLocator() {
            return this.locator;
        }

        /**
         * Receive notification of the beginning of a document.
         *
         * <p>The SAX parser will invoke this method only once, before any
         * other event callbacks (except for {@link #setDocumentLocator
         * setDocumentLocator}).</p>
         *
         * @throws org.xml.sax.SAXException any SAX exception, possibly
         *            wrapping another exception
         * @see #endDocument
         */
        public void startDocument() throws SAXException {
            checkClosed();
        }

        /**
         * Receive notification of the end of a document.
         *
         * <p><strong>There is an apparent contradiction between the
         * documentation for this method and the documentation for {@link
         * org.xml.sax.ErrorHandler#fatalError}.  Until this ambiguity is
         * resolved in a future major release, clients should make no
         * assumptions about whether endDocument() will or will not be
         * invoked when the parser has reported a fatalError() or thrown
         * an exception.</strong></p>
         *
         * <p>The SAX parser will invoke this method only once, and it will
         * be the last method invoked during the parse.  The parser shall
         * not invoke this method until it has either abandoned parsing
         * (because of an unrecoverable error) or reached the end of
         * input.</p>
         *
         * @throws org.xml.sax.SAXException any SAX exception, possibly
         *            wrapping another exception
         * @see #startDocument
         */
        public void endDocument() throws SAXException {
            checkClosed();
        }

        /**
         * Begin the scope of a prefix-URI Namespace mapping.
         *
         * <p>The information from this event is not necessary for
         * normal Namespace processing: the SAX XML reader will
         * automatically replace prefixes for element and attribute
         * names when the <code>http://xml.org/sax/features/namespaces</code>
         * feature is <var>true</var> (the default).</p>
         *
         * <p>There are cases, however, when applications need to
         * use prefixes in character data or in attribute values,
         * where they cannot safely be expanded automatically; the
         * start/endPrefixMapping event supplies the information
         * to the application to expand prefixes in those contexts
         * itself, if necessary.</p>
         *
         * <p>Note that start/endPrefixMapping events are not
         * guaranteed to be properly nested relative to each other:
         * all startPrefixMapping events will occur immediately before the
         * corresponding {@link #startElement startElement} event,
         * and all {@link #endPrefixMapping endPrefixMapping}
         * events will occur immediately after the corresponding
         * {@link #endElement endElement} event,
         * but their order is not otherwise
         * guaranteed.</p>
         *
         * <p>There should never be start/endPrefixMapping events for the
         * "xml" prefix, since it is predeclared and immutable.</p>
         *
         * @param prefix the Namespace prefix being declared.
         *      An empty string is used for the default element namespace,
         *      which has no prefix.
         * @param uri the Namespace URI the prefix is mapped to
         * @throws org.xml.sax.SAXException the client may throw
         *            an exception during processing
         * @see #endPrefixMapping
         * @see #startElement
         */
        public void startPrefixMapping(String prefix,
                                       String uri) throws SAXException {
            checkClosed();
        }

        /**
         * End the scope of a prefix-URI mapping.
         *
         * <p>See {@link #startPrefixMapping startPrefixMapping} for
         * details.  These events will always occur immediately after the
         * corresponding {@link #endElement endElement} event, but the order of
         * {@link #endPrefixMapping endPrefixMapping} events is not otherwise
         * guaranteed.</p>
         *
         * @param prefix the prefix that was being mapped.
         *      This is the empty string when a default mapping scope ends.
         * @throws org.xml.sax.SAXException the client may throw
         *            an exception during processing
         * @see #startPrefixMapping
         * @see #endElement
         */
        public void endPrefixMapping(String prefix) throws SAXException {
            checkClosed();
        }

        /**
         * Receive notification of the beginning of an element.
         *
         * <p>The Parser will invoke this method at the beginning of every
         * element in the XML document; there will be a corresponding
         * {@link #endElement endElement} event for every startElement event
         * (even when the element is empty). All of the element's content will be
         * reported, in order, before the corresponding endElement
         * event.</p>
         *
         * <p>This event allows up to three name components for each
         * element:</p>
         *
         * <ol>
         * <li>the Namespace URI;</li>
         * <li>the local name; and</li>
         * <li>the qualified (prefixed) name.</li>
         * </ol>
         *
         * <p>Any or all of these may be provided, depending on the
         * values of the <var>http://xml.org/sax/features/namespaces</var>
         * and the <var>http://xml.org/sax/features/namespace-prefixes</var>
         * properties:</p>
         *
         * <ul>
         * <li>the Namespace URI and local name are required when
         * the namespaces property is <var>true</var> (the default), and are
         * optional when the namespaces property is <var>false</var> (if one is
         * specified, both must be);</li>
         * <li>the qualified name is required when the namespace-prefixes property
         * is <var>true</var>, and is optional when the namespace-prefixes property
         * is <var>false</var> (the default).</li>
         * </ul>
         *
         * <p>Note that the attribute list provided will contain only
         * attributes with explicit values (specified or defaulted):
         * #IMPLIED attributes will be omitted.  The attribute list
         * will contain attributes used for Namespace declarations
         * (xmlns* attributes) only if the
         * <code>http://xml.org/sax/features/namespace-prefixes</code>
         * property is true (it is false by default, and support for a
         * true value is optional).</p>
         *
         * <p>Like {@link #characters characters()}, attribute values may have
         * characters that need more than one <code>char</code> value.  </p>
         *
         * @param uri the Namespace URI, or the empty string if the
         *        element has no Namespace URI or if Namespace
         *        processing is not being performed
         * @param localName the local name (without prefix), or the
         *        empty string if Namespace processing is not being
         *        performed
         * @param qName the qualified name (with prefix), or the
         *        empty string if qualified names are not available
         * @param atts the attributes attached to the element.  If
         *        there are no attributes, it shall be an empty
         *        Attributes object.  The value of this object after
         *        startElement returns is undefined
         * @throws org.xml.sax.SAXException any SAX exception, possibly
         *            wrapping another exception
         * @see #endElement
         * @see org.xml.sax.Attributes
         * @see org.xml.sax.helpers.AttributesImpl
         */
        public void startElement(String uri, String localName, String qName,
                                 Attributes atts) throws SAXException {

            checkClosed();

            Element element;

            if ((uri == null) || (uri.length() == 0)) {
                element = getDocument().createElement(qName);
            } else {
                element = getDocument().createElementNS(uri, qName);
            }

            if (atts != null) {
                for (int i = 0; i < atts.getLength(); i++) {
                    String attrURI   = atts.getURI(i);
                    String attrQName = atts.getQName(i);
                    String attrValue = atts.getValue(i);

                    if ((attrURI == null) || (attrURI.length() == 0)) {
                        element.setAttribute(attrQName, attrValue);
                    } else {
                        element.setAttributeNS(attrURI, attrQName, attrValue);
                    }
                }
            }
            getCurrentNode().appendChild(element);
            setCurrentNode(element);

            if (getCurrentElement() == null) {
                setCurrentElement(element);
            }
        }

        /**
         * Receive notification of the end of an element.
         *
         * <p>The SAX parser will invoke this method at the end of every
         * element in the XML document; there will be a corresponding
         * {@link #startElement startElement} event for every endElement
         * event (even when the element is empty).</p>
         *
         * <p>For information on the names, see startElement.</p>
         *
         * @param uri the Namespace URI, or the empty string if the
         *        element has no Namespace URI or if Namespace
         *        processing is not being performed
         * @param localName the local name (without prefix), or the
         *        empty string if Namespace processing is not being
         *        performed
         * @param qName the qualified XML name (with prefix), or the
         *        empty string if qualified names are not available
         * @throws org.xml.sax.SAXException any SAX exception, possibly
         *            wrapping another exception
         */
        public void endElement(String uri, String localName,
                               String qName) throws SAXException {
            checkClosed();
            setCurrentNode(getCurrentNode().getParentNode());
        }

        /**
         * Receive notification of character data.
         *
         * <p>The Parser will call this method to report each chunk of
         * character data.  SAX parsers may return all contiguous character
         * data in a single chunk, or they may split it into several
         * chunks; however, all of the characters in any single event
         * must come from the same external entity so that the Locator
         * provides useful information.</p>
         *
         * <p>The application must not attempt to read from the array
         * outside of the specified range.</p>
         *
         * <p>Individual characters may consist of more than one Java
         * <code>char</code> value.  There are two important cases where this
         * happens, because characters can't be represented in just sixteen bits.
         * In one case, characters are represented in a <em>Surrogate Pair</em>,
         * using two special Unicode values. Such characters are in the so-called
         * "Astral Planes", with a code point above U+FFFF.  A second case involves
         * composite characters, such as a base character combining with one or
         * more accent characters. </p>
         *
         * <p> Your code should not assume that algorithms using
         * <code>char</code>-at-a-time idioms will be working in character
         * units; in some cases they will split characters.  This is relevant
         * wherever XML permits arbitrary characters, such as attribute values,
         * processing instruction data, and comments as well as in data reported
         * from this method.  It's also generally relevant whenever Java code
         * manipulates internationalized text; the issue isn't unique to XML.</p>
         *
         * <p>Note that some parsers will report whitespace in element
         * content using the {@link #ignorableWhitespace ignorableWhitespace}
         * method rather than this one (validating parsers <em>must</em>
         * do so).</p>
         *
         * @param ch the characters from the XML document
         * @param start the start position in the array
         * @param length the number of characters to read from the array
         * @throws org.xml.sax.SAXException any SAX exception, possibly
         *            wrapping another exception
         * @see #ignorableWhitespace
         * @see org.xml.sax.Locator
         */
        public void characters(char[] ch, int start,
                               int length) throws SAXException {

            checkClosed();

            Node   node = getCurrentNode().getLastChild();
            String s    = new String(ch, start, length);

            if ((node != null) && (node.getNodeType() == Node.TEXT_NODE)) {
                ((Text) node).appendData(s);
            } else {
                Text text = getDocument().createTextNode(s);

                getCurrentNode().appendChild(text);
            }
        }

        /**
         * Receive notification of ignorable whitespace in element content.
         *
         * <p>Validating Parsers must use this method to report each chunk
         * of whitespace in element content (see the W3C XML 1.0
         * recommendation, section 2.10): non-validating parsers may also
         * use this method if they are capable of parsing and using
         * content models.</p>
         *
         * <p>SAX parsers may return all contiguous whitespace in a single
         * chunk, or they may split it into several chunks; however, all of
         * the characters in any single event must come from the same
         * external entity, so that the Locator provides useful
         * information.</p>
         *
         * <p>The application must not attempt to read from the array
         * outside of the specified range.</p>
         *
         * @param ch the characters from the XML document
         * @param start the start position in the array
         * @param length the number of characters to read from the array
         * @throws org.xml.sax.SAXException any SAX exception, possibly
         *            wrapping another exception
         * @see #characters
         */
        public void ignorableWhitespace(char[] ch, int start,
                                        int length) throws SAXException {
            characters(ch, start, length);
        }

        /**
         * Receive notification of a processing instruction.
         *
         * <p>The Parser will invoke this method once for each processing
         * instruction found: note that processing instructions may occur
         * before or after the main document element.</p>
         *
         * <p>A SAX parser must never report an XML declaration (XML 1.0,
         * section 2.8) or a text declaration (XML 1.0, section 4.3.1)
         * using this method.</p>
         *
         * <p>Like {@link #characters characters()}, processing instruction
         * data may have characters that need more than one <code>char</code>
         * value. </p>
         *
         * @param target the processing instruction target
         * @param data the processing instruction data, or null if
         *        none was supplied.  The data does not include any
         *        whitespace separating it from the target
         * @throws org.xml.sax.SAXException any SAX exception, possibly
         *            wrapping another exception
         */
        public void processingInstruction(String target,
                String data) throws SAXException {

            checkClosed();

            ProcessingInstruction processingInstruction;

            processingInstruction =
                getDocument().createProcessingInstruction(target, data);

            getCurrentNode().appendChild(processingInstruction);
        }

        /**
         * Receive notification of a skipped entity.
         * This is not called for entity references within markup constructs
         * such as element start tags or markup declarations.  (The XML
         * recommendation requires reporting skipped external entities.
         * SAX also reports internal entity expansion/non-expansion, except
         * within markup constructs.)
         *
         * <p>The Parser will invoke this method each time the entity is
         * skipped.  Non-validating processors may skip entities if they
         * have not seen the declarations (because, for example, the
         * entity was declared in an external DTD subset).  All processors
         * may skip external entities, depending on the values of the
         * <code>http://xml.org/sax/features/external-general-entities</code>
         * and the
         * <code>http://xml.org/sax/features/external-parameter-entities</code>
         * properties.</p>
         *
         * @param name the name of the skipped entity.  If it is a
         *        parameter entity, the name will begin with '%', and if
         *        it is the external DTD subset, it will be the string
         *        "[dtd]"
         * @throws org.xml.sax.SAXException any SAX exception, possibly
         *            wrapping another exception
         */
        public void skippedEntity(String name) throws SAXException {

            checkClosed();

            EntityReference entityReference =
                getDocument().createEntityReference(name);

            getCurrentNode().appendChild(entityReference);
        }

        /**
         * Closes this DOMBuilder.
         */
        public void close() {

            this.closed         = true;
            this.document       = null;
            this.currentElement = null;
            this.currentNode    = null;
            this.locator        = null;
        }

        /**
         * Retrieves whether this DOMBuilder is closed.
         */
        public boolean isClosed() {
            return this.closed;
        }

        /**
         * Checks whether this DOMBuilder is closed.
         *
         * @throws SAXException if this DOMBuilder is closed.
         */
        protected void checkClosed() throws SAXException {

            if (isClosed()) {
                throw new SAXException("content handler is closed.");    // NOI18N
            }
        }

        /**
         * Retrieves the document. <p>
         */
        protected Document getDocument() {
            return this.document;
        }

        /**
         * Retreives the current element. <p>
         */
        protected Element getCurrentElement() {
            return this.currentElement;
        }

        /**
         * Assigns the current element.
         * @param element
         */
        protected void setCurrentElement(Element element) {
            this.currentElement = element;
        }

        /**
         * Retrieves the current node. <p>
         */
        protected Node getCurrentNode() {
            return this.currentNode;
        }

        /**
         * Assigns the current node. <p>
         * @param node
         */
        protected void setCurrentNode(Node node) {
            this.currentNode = node;
        }
    }

    /**
     * Writes to a {@link javax.xml.stream.XMLStreamWriter XMLStreamWriter}
     * from SAX events.
     */
    public static class SAX2XMLStreamWriter implements ContentHandler,
            Closeable {

        /**
         * Namespace declarations for an upcoming element.
         */
        private List<QualifiedName> namespaces =
            new ArrayList<QualifiedName>();

        /**
         * Whether this object is closed.
         */
        private boolean closed;

        /**
         * This object's SAX locator.
         */
        private Locator locator;

        /**
         * XML stream writer where events are pushed.
         */
        private XMLStreamWriter writer;

        /**
         * Constructs a new SAX2XMLStreamWriter that writes SAX events to the
         * designated XMLStreamWriter.
         *
         * @param writer the writer to which to write SAX events
         */
        public SAX2XMLStreamWriter(XMLStreamWriter writer) {

            if (writer == null) {
                throw new NullPointerException("writer");
            }
            this.writer = writer;
        }

        /**
         * Receive notification of the beginning of a document.
         *
         * <p>The SAX parser will invoke this method only once, before any
         * other event callbacks (except for {@link #setDocumentLocator
         * setDocumentLocator}).</p>
         *
         * @throws org.xml.sax.SAXException any SAX exception, possibly
         *            wrapping another exception
         * @see #endDocument
         */
        public void startDocument() throws SAXException {

            checkClosed();

            try {
                this.writer.writeStartDocument();
            } catch (XMLStreamException e) {
                throw new SAXException(e);
            }
        }

        /**
         * Receive notification of the end of a document.
         *
         * <p><strong>There is an apparent contradiction between the
         * documentation for this method and the documentation for {@link
         * org.xml.sax.ErrorHandler#fatalError}.  Until this ambiguity is
         * resolved in a future major release, clients should make no
         * assumptions about whether endDocument() will or will not be
         * invoked when the parser has reported a fatalError() or thrown
         * an exception.</strong></p>
         *
         * <p>The SAX parser will invoke this method only once, and it will
         * be the last method invoked during the parse.  The parser shall
         * not invoke this method until it has either abandoned parsing
         * (because of an unrecoverable error) or reached the end of
         * input.</p>
         *
         * @throws org.xml.sax.SAXException any SAX exception, possibly
         *            wrapping another exception
         * @see #startDocument
         */
        public void endDocument() throws SAXException {

            checkClosed();

            try {
                this.writer.writeEndDocument();
                this.writer.flush();
            } catch (XMLStreamException e) {
                throw new SAXException(e);
            }
        }

        /**
         * Receive notification of character data.
         *
         * <p>The Parser will call this method to report each chunk of
         * character data.  SAX parsers may return all contiguous character
         * data in a single chunk, or they may split it into several
         * chunks; however, all of the characters in any single event
         * must come from the same external entity so that the Locator
         * provides useful information.</p>
         *
         * <p>The application must not attempt to read from the array
         * outside of the specified range.</p>
         *
         * <p>Individual characters may consist of more than one Java
         * <code>char</code> value.  There are two important cases where this
         * happens, because characters can't be represented in just sixteen bits.
         * In one case, characters are represented in a <em>Surrogate Pair</em>,
         * using two special Unicode values. Such characters are in the so-called
         * "Astral Planes", with a code point above U+FFFF.  A second case involves
         * composite characters, such as a base character combining with one or
         * more accent characters. </p>
         *
         * <p> Your code should not assume that algorithms using
         * <code>char</code>-at-a-time idioms will be working in character
         * units; in some cases they will split characters.  This is relevant
         * wherever XML permits arbitrary characters, such as attribute values,
         * processing instruction data, and comments as well as in data reported
         * from this method.  It's also generally relevant whenever Java code
         * manipulates internationalized text; the issue isn't unique to XML.</p>
         *
         * <p>Note that some parsers will report whitespace in element
         * content using the {@link #ignorableWhitespace ignorableWhitespace}
         * method rather than this one (validating parsers <em>must</em>
         * do so).</p>
         *
         * @param ch the characters from the XML document
         * @param start the start position in the array
         * @param length the number of characters to read from the array
         * @throws org.xml.sax.SAXException any SAX exception, possibly
         *            wrapping another exception
         * @see #ignorableWhitespace
         * @see org.xml.sax.Locator
         */
        public void characters(char[] ch, int start,
                               int length) throws SAXException {

            checkClosed();

            try {
                this.writer.writeCharacters(ch, start, length);
            } catch (XMLStreamException e) {
                throw new SAXException(e);
            }
        }

        /**
         * Receive notification of the beginning of an element.
         *
         * <p>The Parser will invoke this method at the beginning of every
         * element in the XML document; there will be a corresponding
         * {@link #endElement endElement} event for every startElement event
         * (even when the element is empty). All of the element's content will be
         * reported, in order, before the corresponding endElement
         * event.</p>
         *
         * <p>This event allows up to three name components for each
         * element:</p>
         *
         * <ol>
         * <li>the Namespace URI;</li>
         * <li>the local name; and</li>
         * <li>the qualified (prefixed) name.</li>
         * </ol>
         *
         * <p>Any or all of these may be provided, depending on the
         * values of the <var>http://xml.org/sax/features/namespaces</var>
         * and the <var>http://xml.org/sax/features/namespace-prefixes</var>
         * properties:</p>
         *
         * <ul>
         * <li>the Namespace URI and local name are required when
         * the namespaces property is <var>true</var> (the default), and are
         * optional when the namespaces property is <var>false</var> (if one is
         * specified, both must be);</li>
         * <li>the qualified name is required when the namespace-prefixes property
         * is <var>true</var>, and is optional when the namespace-prefixes property
         * is <var>false</var> (the default).</li>
         * </ul>
         *
         * <p>Note that the attribute list provided will contain only
         * attributes with explicit values (specified or defaulted):
         * #IMPLIED attributes will be omitted.  The attribute list
         * will contain attributes used for Namespace declarations
         * (xmlns* attributes) only if the
         * <code>http://xml.org/sax/features/namespace-prefixes</code>
         * property is true (it is false by default, and support for a
         * true value is optional).</p>
         *
         * <p>Like {@link #characters characters()}, attribute values may have
         * characters that need more than one <code>char</code> value.  </p>
         *
         * @param namespaceURI the Namespace URI, or the empty string if the
         *        element has no Namespace URI or if Namespace
         *        processing is not being performed
         * @param localName the local name (without prefix), or the
         *        empty string if Namespace processing is not being
         *        performed
         * @param qName the qualified name (with prefix), or the
         *        empty string if qualified names are not available
         * @param atts the attributes attached to the element.  If
         *        there are no attributes, it shall be an empty
         *        Attributes object.  The value of this object after
         *        startElement returns is undefined
         * @throws org.xml.sax.SAXException any SAX exception, possibly
         *            wrapping another exception
         * @see #endElement
         * @see org.xml.sax.Attributes
         * @see org.xml.sax.helpers.AttributesImpl
         */
        public void startElement(String namespaceURI, String localName,
                                 String qName,
                                 Attributes atts) throws SAXException {

            checkClosed();

            try {
                int    qi     = qName.indexOf(':');
                String prefix = (qi > 0) ? qName.substring(0, qi)
                        : "";

                this.writer.writeStartElement(prefix, localName, namespaceURI);

                int length = namespaces.size();

                for (int i = 0; i < length; i++) {
                    QualifiedName ns = namespaces.get(i);

                    this.writer.writeNamespace(ns.prefix, ns.namespaceName);
                }
                namespaces.clear();

                length = atts.getLength();

                for (int i = 0; i < length; i++) {
                    this.writer.writeAttribute(atts.getURI(i),
                            atts.getLocalName(i), atts.getValue(i));
                }
            } catch (XMLStreamException e) {
                throw new SAXException(e);
            }
        }

        /**
         * Receive notification of the end of an element.
         *
         * <p>The SAX parser will invoke this method at the end of every
         * element in the XML document; there will be a corresponding
         * {@link #startElement startElement} event for every endElement
         * event (even when the element is empty).</p>
         *
         * <p>For information on the names, see startElement.</p>
         *
         * @param namespaceURI the Namespace URI, or the empty string if the
         *        element has no Namespace URI or if Namespace
         *        processing is not being performed
         * @param localName the local name (without prefix), or the
         *        empty string if Namespace processing is not being
         *        performed
         * @param qName the qualified XML name (with prefix), or the
         *        empty string if qualified names are not available
         * @throws org.xml.sax.SAXException any SAX exception, possibly
         *            wrapping another exception
         */
        public void endElement(String namespaceURI, String localName,
                               String qName) throws SAXException {

            checkClosed();

            try {
                this.writer.writeEndElement();
            } catch (XMLStreamException e) {
                throw new SAXException(e);
            }
        }

        /**
         * Begin the scope of a prefix-URI Namespace mapping.
         *
         * <p>The information from this event is not necessary for
         * normal Namespace processing: the SAX XML reader will
         * automatically replace prefixes for element and attribute
         * names when the <code>http://xml.org/sax/features/namespaces</code>
         * feature is <var>true</var> (the default).</p>
         *
         * <p>There are cases, however, when applications need to
         * use prefixes in character data or in attribute values,
         * where they cannot safely be expanded automatically; the
         * start/endPrefixMapping event supplies the information
         * to the application to expand prefixes in those contexts
         * itself, if necessary.</p>
         *
         * <p>Note that start/endPrefixMapping events are not
         * guaranteed to be properly nested relative to each other:
         * all startPrefixMapping events will occur immediately before the
         * corresponding {@link #startElement startElement} event,
         * and all {@link #endPrefixMapping endPrefixMapping}
         * events will occur immediately after the corresponding
         * {@link #endElement endElement} event,
         * but their order is not otherwise
         * guaranteed.</p>
         *
         * <p>There should never be start/endPrefixMapping events for the
         * "xml" prefix, since it is predeclared and immutable.</p>
         *
         * @param prefix the Namespace prefix being declared.
         *      An empty string is used for the default element namespace,
         *      which has no prefix.
         * @param uri the Namespace URI the prefix is mapped to
         * @throws org.xml.sax.SAXException the client may throw
         *            an exception during processing
         * @see #endPrefixMapping
         * @see #startElement
         */
        public void startPrefixMapping(String prefix,
                                       String uri) throws SAXException {

            checkClosed();

            try {
                this.writer.setPrefix(prefix, uri);
                namespaces.add(new QualifiedName(prefix, uri));
            } catch (XMLStreamException e) {
                throw new SAXException(e);
            }
        }

        /**
         * End the scope of a prefix-URI mapping.
         *
         * <p>See {@link #startPrefixMapping startPrefixMapping} for
         * details.  These events will always occur immediately after the
         * corresponding {@link #endElement endElement} event, but the order of
         * {@link #endPrefixMapping endPrefixMapping} events is not otherwise
         * guaranteed.</p>
         *
         * @param prefix the prefix that was being mapped.
         *      This is the empty string when a default mapping scope ends.
         * @throws org.xml.sax.SAXException the client may throw
         *            an exception during processing
         * @see #startPrefixMapping
         * @see #endElement
         */
        public void endPrefixMapping(String prefix) throws SAXException {

            checkClosed();

            //
        }

        /**
         * Receive notification of ignorable whitespace in element content.
         *
         * <p>Validating Parsers must use this method to report each chunk
         * of whitespace in element content (see the W3C XML 1.0
         * recommendation, section 2.10): non-validating parsers may also
         * use this method if they are capable of parsing and using
         * content models.</p>
         *
         * <p>SAX parsers may return all contiguous whitespace in a single
         * chunk, or they may split it into several chunks; however, all of
         * the characters in any single event must come from the same
         * external entity, so that the Locator provides useful
         * information.</p>
         *
         * <p>The application must not attempt to read from the array
         * outside of the specified range.</p>
         *
         * @param ch the characters from the XML document
         * @param start the start position in the array
         * @param length the number of characters to read from the array
         * @throws org.xml.sax.SAXException any SAX exception, possibly
         *            wrapping another exception
         * @see #characters
         */
        public void ignorableWhitespace(char[] ch, int start,
                                        int length) throws SAXException {
            characters(ch, start, length);
        }

        /**
         * Receive notification of a processing instruction.
         *
         * <p>The Parser will invoke this method once for each processing
         * instruction found: note that processing instructions may occur
         * before or after the main document element.</p>
         *
         * <p>A SAX parser must never report an XML declaration (XML 1.0,
         * section 2.8) or a text declaration (XML 1.0, section 4.3.1)
         * using this method.</p>
         *
         * <p>Like {@link #characters characters()}, processing instruction
         * data may have characters that need more than one <code>char</code>
         * value. </p>
         *
         * @param target the processing instruction target
         * @param data the processing instruction data, or null if
         *        none was supplied.  The data does not include any
         *        whitespace separating it from the target
         * @throws org.xml.sax.SAXException any SAX exception, possibly
         *            wrapping another exception
         */
        public void processingInstruction(String target,
                String data) throws SAXException {

            checkClosed();

            try {
                this.writer.writeProcessingInstruction(target, data);
            } catch (XMLStreamException e) {
                throw new SAXException(e);
            }
        }

        /**
         * Receive an object for locating the origin of SAX document events.
         *
         * <p>SAX parsers are strongly encouraged (though not absolutely
         * required) to supply a locator: if it does so, it must supply
         * the locator to the application by invoking this method before
         * invoking any of the other methods in the ContentHandler
         * interface.</p>
         *
         * <p>The locator allows the application to determine the end
         * position of any document-related event, even if the parser is
         * not reporting an error.  Typically, the application will
         * use this information for reporting its own errors (such as
         * character content that does not match an application's
         * business rules).  The information returned by the locator
         * is probably not sufficient for use with a search engine.</p>
         *
         * <p>Note that the locator will return correct information only
         * during the invocation SAX event callbacks after
         * {@link #startDocument startDocument} returns and before
         * {@link #endDocument endDocument} is called.  The
         * application should not attempt to use it at any other time.</p>
         *
         * @param locator an object that can return the location of
         *                any SAX document event
         * @see org.xml.sax.Locator
         */
        public void setDocumentLocator(Locator locator) {
            this.locator = locator;
        }

        /**
         * Retrieves the Locator. <p>
         * @return the Locator
         */
        public Locator getDocumentLocator() {
            return this.locator;
        }

        /**
         * Receive notification of a skipped entity.
         * This is not called for entity references within markup constructs
         * such as element start tags or markup declarations.  (The XML
         * recommendation requires reporting skipped external entities.
         * SAX also reports internal entity expansion/non-expansion, except
         * within markup constructs.)
         *
         * <p>The Parser will invoke this method each time the entity is
         * skipped.  Non-validating processors may skip entities if they
         * have not seen the declarations (because, for example, the
         * entity was declared in an external DTD subset).  All processors
         * may skip external entities, depending on the values of the
         * <code>http://xml.org/sax/features/external-general-entities</code>
         * and the
         * <code>http://xml.org/sax/features/external-parameter-entities</code>
         * properties.</p>
         *
         * @param name the name of the skipped entity.  If it is a
         *        parameter entity, the name will begin with '%', and if
         *        it is the external DTD subset, it will be the string
         *        "[dtd]"
         * @throws org.xml.sax.SAXException any SAX exception, possibly
         *            wrapping another exception
         */
        public void skippedEntity(String name) throws SAXException {

            checkClosed();

            //
        }

        public void comment(char[] ch, int start,
                            int length) throws SAXException {

            checkClosed();

            try {
                this.writer.writeComment(new String(ch, start, length));
            } catch (XMLStreamException e) {
                throw new SAXException(e);
            }
        }

        public XMLStreamWriter getWriter() {
            return this.writer;
        }

        protected List<QualifiedName> getNamespaces() {
            return this.namespaces;
        }

        /**
         * Closes this object.
         */
        public void close() throws IOException {

            if (!this.closed) {
                this.closed = true;

                try {
                    this.writer.close();
                } catch (XMLStreamException e) {
                    throw new IOException(e);
                } finally {
                    this.writer     = null;
                    this.locator    = null;
                    this.namespaces = null;
                }
            }
        }

        /**
         * Retrieves whether this object is closed.
         */
        public boolean isClosed() {
            return this.closed;
        }

        /**
         * Checks whether this object is closed.
         *
         * @throws SAXException if this DOMBuilder is closed.
         */
        protected void checkClosed() throws SAXException {

            if (isClosed()) {
                throw new SAXException("content handler is closed.");    // NOI18N
            }
        }

        // --------------------- internal implementation -----------------------
        protected class QualifiedName {

            public final String namespaceName;
            public final String prefix;

            public QualifiedName(final String prefix,
                                 final String namespaceName) {
                this.prefix        = prefix;
                this.namespaceName = namespaceName;
            }
        }
    }
}
