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
 */package org.voltdb.sqlparser.tools.model;

import java.io.File;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

/**
 * This object contains basic implementations to load an XML file into java
 * objects and to save an object graph into an xml file
 *
 */
public class StringParser {

    private JAXBContext jaxbContext;
    private ObjectFactory objectFactory = new ObjectFactory();

    public String load(String xmlFile) throws JAXBException {
        Unmarshaller unmarshaller = this.getContext().createUnmarshaller();
        @SuppressWarnings("unchecked")
        JAXBElement<String> rootElement = (JAXBElement<String>) unmarshaller.unmarshal(new File(xmlFile));

        return rootElement.getValue();
    }

    public void save(String rootElement, String xmlFile) throws JAXBException {
        this.save(rootElement, xmlFile, "UTF-8");
    }

    public void save(String rootElement, String xmlFile, String encoding) throws JAXBException {
        JAXBElement<String> jaxbElement = this.objectFactory.createDdl(rootElement);
        Marshaller marshaller = this.getContext().createMarshaller();

        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        marshaller.setProperty(Marshaller.JAXB_ENCODING, encoding);
        marshaller.marshal(jaxbElement, new File(xmlFile));
    }

    private JAXBContext getContext() throws JAXBException {
        if (this.jaxbContext == null) {
            this.jaxbContext = JAXBContext.newInstance(String.class.getPackage().getName());
        }
        return this.jaxbContext;
    }

}
