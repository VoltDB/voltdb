
package org.voltdb.sqlparser.tools.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;


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
 *         &lt;element ref="{http://www.voltdb.org}ddl" maxOccurs="unbounded"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "ddl"
})
@XmlRootElement(name = "schema")
public class Schema {

    @XmlElement(required = true)
    protected List<String> ddl;

    /**
     * Default no-arg constructor
     * 
     */
    public Schema() {
        super();
    }

    /**
     * Fully-initialising value constructor
     * 
     */
    public Schema(final List<String> ddl) {
        this.ddl = ddl;
    }

    /**
     * Gets the value of the ddl property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the ddl property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getDdl().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link String }
     * 
     * 
     */
    public List<String> getDdl() {
        if (ddl == null) {
            ddl = new ArrayList<String>();
        }
        return this.ddl;
    }

    public Schema withDdl(String... values) {
        if (values!= null) {
            for (String value: values) {
                getDdl().add(value);
            }
        }
        return this;
    }

    public Schema withDdl(Collection<String> values) {
        if (values!= null) {
            getDdl().addAll(values);
        }
        return this;
    }

}
