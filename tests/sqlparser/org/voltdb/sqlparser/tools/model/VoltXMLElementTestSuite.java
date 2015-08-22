
package org.voltdb.sqlparser.tools.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSchemaType;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.CollapsedStringAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;


/**
 * <p>Java class for anonymous complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.voltdb.org}tests"/>
 *       &lt;/sequence>
 *       &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}NCName" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "tests"
})
@XmlRootElement(name = "VoltXMLElementTestSuite")
public class VoltXMLElementTestSuite {

    @XmlElement(required = true)
    protected Tests tests;
    @XmlAttribute(name = "name", required = true)
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    @XmlSchemaType(name = "NCName")
    protected String name;

    /**
     * Default no-arg constructor
     * 
     */
    public VoltXMLElementTestSuite() {
        super();
    }

    /**
     * Fully-initialising value constructor
     * 
     */
    public VoltXMLElementTestSuite(final Tests tests, final String name) {
        this.tests = tests;
        this.name = name;
    }

    /**
     * Gets the value of the tests property.
     * 
     * @return
     *     possible object is
     *     {@link Tests }
     *     
     */
    public Tests getTests() {
        return tests;
    }

    /**
     * Sets the value of the tests property.
     * 
     * @param value
     *     allowed object is
     *     {@link Tests }
     *     
     */
    public void setTests(Tests value) {
        this.tests = value;
    }

    /**
     * Gets the value of the name property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the value of the name property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setName(String value) {
        this.name = value;
    }

    public VoltXMLElementTestSuite withTests(Tests value) {
        setTests(value);
        return this;
    }

    public VoltXMLElementTestSuite withName(String value) {
        setName(value);
        return this;
    }

}
