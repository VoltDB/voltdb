
package org.voltdb.sqlparser.tools.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
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
 *       &lt;attribute name="testKind" use="required" type="{http://www.w3.org/2001/XMLSchema}NCName" />
 *       &lt;attribute name="testName" use="required" type="{http://www.w3.org/2001/XMLSchema}NCName" />
 *       &lt;attribute name="comment" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="testSQL" use="required" type="{http://www.w3.org/2001/XMLSchema}anySimpleType" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "")
@XmlRootElement(name = "testpoint")
public class Testpoint {

    @XmlAttribute(name = "testKind", required = true)
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    @XmlSchemaType(name = "NCName")
    protected String testKind;
    @XmlAttribute(name = "testName", required = true)
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    @XmlSchemaType(name = "NCName")
    protected String testName;
    @XmlAttribute(name = "comment")
    protected String comment;
    @XmlAttribute(name = "testSQL", required = true)
    @XmlSchemaType(name = "anySimpleType")
    protected String testSQL;

    /**
     * Default no-arg constructor
     * 
     */
    public Testpoint() {
        super();
    }

    /**
     * Fully-initialising value constructor
     * 
     */
    public Testpoint(final String testKind, final String testName, final String comment, final String testSQL) {
        this.testKind = testKind;
        this.testName = testName;
        this.comment = comment;
        this.testSQL = testSQL;
    }

    /**
     * Gets the value of the testKind property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getTestKind() {
        return testKind;
    }

    /**
     * Sets the value of the testKind property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setTestKind(String value) {
        this.testKind = value;
    }

    /**
     * Gets the value of the testName property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getTestName() {
        return testName;
    }

    /**
     * Sets the value of the testName property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setTestName(String value) {
        this.testName = value;
    }

    /**
     * Gets the value of the comment property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getComment() {
        return comment;
    }

    /**
     * Sets the value of the comment property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setComment(String value) {
        this.comment = value;
    }

    /**
     * Gets the value of the testSQL property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getTestSQL() {
        return testSQL;
    }

    /**
     * Sets the value of the testSQL property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setTestSQL(String value) {
        this.testSQL = value;
    }

    public Testpoint withTestKind(String value) {
        setTestKind(value);
        return this;
    }

    public Testpoint withTestName(String value) {
        setTestName(value);
        return this;
    }

    public Testpoint withComment(String value) {
        setComment(value);
        return this;
    }

    public Testpoint withTestSQL(String value) {
        setTestSQL(value);
        return this;
    }

}
