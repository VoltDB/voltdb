
package org.voltdb.sqlparser.tools.model;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the org.voltdb.sqlparser.tools.model package. 
 * <p>An ObjectFactory allows you to programatically 
 * construct new instances of the Java representation 
 * for XML content. The Java representation of XML 
 * content can consist of schema derived interfaces 
 * and classes representing the binding of schema 
 * type definitions, element declarations and model 
 * groups.  Factory methods for each of these are 
 * provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {

    private final static QName _Ddl_QNAME = new QName("http://www.voltdb.org", "ddl");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: org.voltdb.sqlparser.tools.model
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link Schema }
     * 
     */
    public Schema createSchema() {
        return new Schema();
    }

    /**
     * Create an instance of {@link VoltXMLElementTestSuite }
     * 
     */
    public VoltXMLElementTestSuite createVoltXMLElementTestSuite() {
        return new VoltXMLElementTestSuite();
    }

    /**
     * Create an instance of {@link Tests }
     * 
     */
    public Tests createTests() {
        return new Tests();
    }

    /**
     * Create an instance of {@link Test }
     * 
     */
    public Test createTest() {
        return new Test();
    }

    /**
     * Create an instance of {@link Testpoint }
     * 
     */
    public Testpoint createTestpoint() {
        return new Testpoint();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.voltdb.org", name = "ddl")
    public JAXBElement<String> createDdl(String value) {
        return new JAXBElement<String>(_Ddl_QNAME, String.class, null, value);
    }

}
