/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.utils;

import java.io.File;
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.voltdb.DependencyPair;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.xml.sax.SAXException;

public class CatalogPasswordScrambler {

    public static DeploymentType getDeployment(File sourceFH) {
        try {
            JAXBContext jc = JAXBContext.newInstance("org.voltdb.compiler.deploymentfile");
            // This schema shot the sheriff.
            SchemaFactory sf = SchemaFactory.newInstance(javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = sf.newSchema(
                    DependencyPair.class.getResource("compiler/DeploymentFileSchema.xsd")
                    );
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            unmarshaller.setSchema(schema);
            @SuppressWarnings("unchecked")
            JAXBElement<DeploymentType> result =
                (JAXBElement<DeploymentType>) unmarshaller.unmarshal(sourceFH);
            DeploymentType deployment = result.getValue();
            return deployment;

        } catch (JAXBException e) {
            // Convert some linked exceptions to more friendly errors.
            if (e.getLinkedException() instanceof java.io.FileNotFoundException) {
                System.err.println(e.getLinkedException().getMessage());
                return null;
            } else if (e.getLinkedException() instanceof org.xml.sax.SAXParseException) {
                System.err.println(
                        "Error schema validating deployment.xml file. " +
                        e.getLinkedException().getMessage()
                        );
                return null;
            } else {
                throw new RuntimeException(e);
            }
        } catch (SAXException e) {
            System.err.println("Error schema validating deployment.xml file. " + e.getMessage());
            return null;
        }
    }

    public static void scramblePasswords(DeploymentType depl) {
        UsersType users = depl.getUsers();
        if (users == null) return;

        for (UsersType.User user: users.getUser()) {
            if (    user.isPlaintext() &&
                    user.getPassword() != null &&
                    !user.getPassword().trim().isEmpty()
            ) {
                user.setPassword(Digester.sha1AsHex(user.getPassword()));
                user.setPlaintext(false);
            }
        }
    }

    public static void writeOutMaskedDeploymentFile(DeploymentType depl, File maskedFH) {
        try {
            org.voltdb.compiler.deploymentfile.ObjectFactory factory =
                    new org.voltdb.compiler.deploymentfile.ObjectFactory();

            JAXBContext context = JAXBContext.newInstance(DeploymentType.class);
            JAXBElement<DeploymentType> doc = factory.createDeployment(depl);

            Marshaller marshaller = context.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,
                                   Boolean.TRUE);
            marshaller.marshal(doc, maskedFH);
        } catch (JAXBException e) {
            String msg = e.getMessage();
            if (msg == null) msg = e.getLinkedException().getMessage();
            System.err.println("Failed to write masked deployemnt: " + msg );
            e.printStackTrace();
        }
    }

    public static void main(String [] args) {
        if (args.length == 1) args = new String[] {args[0], args[0]};
        if (args.length != 2) {
            System.out.println("Usage: CatalogPasswordScrambler [deployment-file-spec] [masked-deployment-file-spec");
        }

        File deployFH = new File(args[0]);
        if (!deployFH.exists() || !deployFH.isFile() || !deployFH.canRead()) {
            System.err.println("cannot access: "+ deployFH);
        }

        File maskedFH = new File(args[1]);
        try {
            File canonMaskedFH = maskedFH.getCanonicalFile();
            File parentFH = canonMaskedFH.getParentFile();
            if (!parentFH.exists() || !parentFH.isDirectory() || !parentFH.canWrite()) {
                System.err.println("do not have write access to " + parentFH);
                return;
            }
        } catch (IOException e) {
            System.err.println("could not stat specified files: " + e.getMessage());
            return;
        }

        DeploymentType depl = getDeployment(deployFH);
        if (depl == null) return;

        try {
            scramblePasswords(depl);
        } catch (UnsupportedOperationException e) {
            System.err.println(e.getMessage());
            return;
        }

        writeOutMaskedDeploymentFile(depl, maskedFH);
    }
}
