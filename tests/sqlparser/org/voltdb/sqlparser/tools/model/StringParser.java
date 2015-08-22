package org.voltdb.sqlparser.tools.model;

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
