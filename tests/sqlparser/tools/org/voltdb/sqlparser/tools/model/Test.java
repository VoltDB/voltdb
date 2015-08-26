/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.voltdb.sqlparser.tools.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
 *         &lt;element ref="{http://www.voltdb.org}schema"/>
 *         &lt;element ref="{http://www.voltdb.org}testpoint" maxOccurs="unbounded"/>
 *       &lt;/sequence>
 *       &lt;attribute name="classname" use="required" type="{http://www.w3.org/2001/XMLSchema}anySimpleType" />
 *       &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}NCName" />
 *       &lt;attribute name="sourcefolder" use="required" type="{http://www.w3.org/2001/XMLSchema}anySimpleType" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "schema",
    "testpoint"
})
@XmlRootElement(name = "test")
public class Test {

    @XmlElement(required = true)
    protected Schema schema;
    @XmlElement(required = true)
    protected List<Testpoint> testpoint;
    @XmlAttribute(name = "classname", required = true)
    @XmlSchemaType(name = "anySimpleType")
    protected String classname;
    @XmlAttribute(name = "name", required = true)
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    @XmlSchemaType(name = "NCName")
    protected String name;
    @XmlAttribute(name = "sourcefolder", required = true)
    @XmlSchemaType(name = "anySimpleType")
    protected String sourcefolder;

    /**
     * Default no-arg constructor
     *
     */
    public Test() {
        super();
    }

    /**
     * Fully-initialising value constructor
     *
     */
    public Test(final Schema schema, final List<Testpoint> testpoint, final String classname, final String name, final String sourcefolder) {
        this.schema = schema;
        this.testpoint = testpoint;
        this.classname = classname;
        this.name = name;
        this.sourcefolder = sourcefolder;
    }

    /**
     * Gets the value of the schema property.
     *
     * @return
     *     possible object is
     *     {@link Schema }
     *
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Sets the value of the schema property.
     *
     * @param value
     *     allowed object is
     *     {@link Schema }
     *
     */
    public void setSchema(Schema value) {
        this.schema = value;
    }

    /**
     * Gets the value of the testpoint property.
     *
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the testpoint property.
     *
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getTestpoint().add(newItem);
     * </pre>
     *
     *
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link Testpoint }
     *
     *
     */
    public List<Testpoint> getTestpoint() {
        if (testpoint == null) {
            testpoint = new ArrayList<Testpoint>();
        }
        return this.testpoint;
    }

    /**
     * Gets the value of the classname property.
     *
     * @return
     *     possible object is
     *     {@link String }
     *
     */
    public String getClassname() {
        return classname;
    }

    /**
     * Sets the value of the classname property.
     *
     * @param value
     *     allowed object is
     *     {@link String }
     *
     */
    public void setClassname(String value) {
        this.classname = value;
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

    /**
     * Gets the value of the sourcefolder property.
     *
     * @return
     *     possible object is
     *     {@link String }
     *
     */
    public String getSourcefolder() {
        return sourcefolder;
    }

    /**
     * Sets the value of the sourcefolder property.
     *
     * @param value
     *     allowed object is
     *     {@link String }
     *
     */
    public void setSourcefolder(String value) {
        this.sourcefolder = value;
    }

    public Test withSchema(Schema value) {
        setSchema(value);
        return this;
    }

    public Test withTestpoint(Testpoint... values) {
        if (values!= null) {
            for (Testpoint value: values) {
                getTestpoint().add(value);
            }
        }
        return this;
    }

    public Test withTestpoint(Collection<Testpoint> values) {
        if (values!= null) {
            getTestpoint().addAll(values);
        }
        return this;
    }

    public Test withClassname(String value) {
        setClassname(value);
        return this;
    }

    public Test withName(String value) {
        setName(value);
        return this;
    }

    public Test withSourcefolder(String value) {
        setSourcefolder(value);
        return this;
    }

}
