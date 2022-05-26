/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Procedure;
import org.voltdb.compiler.CatalogChangeResult;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.utils.InMemoryJarfile;

/**
 * Base class for catalog validators. The fully qualified name of derived implementations
 * must be added to {@code s_implementations} in order to be instantiated at startup time.
 * <P>
 * Implementations must only have an empty constructor. Only one instance of each implementation
 * will be created, and invoked at startup time and on each catalog update.
 * <P>
 * The {@link CatalogValidator#validateConfiguration} method allows validating the whole new
 * {@link Catalog}, allowing checking consistency across different object types. Exceptionally,
 * this method can also be used to perform late updates of the catalog objects, e.g. when
 * updating {@link org.voltdb.catalog.Topic} instances with schemas registered to an external
 * server from information collected in the {@link org.voltdb.catalog.Table} instance associated
 * with the topic.
 */
public class CatalogValidator {

    private static class Entry {
        final String m_implementation;
        final boolean m_isPro;

        Entry(String implementation, boolean isPro) {
            m_implementation = implementation;
            m_isPro = isPro;
        }

        String getImplementation() {
            return m_implementation;
        }

        boolean isPro() {
            return m_isPro;
        }
    }
    private static Entry[] s_implementations = {
            new Entry("org.voltdb.e3.topics.TopicsValidator", true),
            new Entry("org.voltdb.e3.topics.TopicsGatewayValidator", true),
            new Entry("org.voltdb.task.TaskValidator", false),
            new Entry("org.voltdb.dr2.DRCatalogValidator", true),
            new Entry("org.voltdb.iv2.PriorityPolicyValidator", false)
    };

    /**
     * Return the list of classes implementing this.
     * <p>
     * Failure to instantiate any class listed in {@code s_implementations} will crash VoltDB.
     *
     * @param includePro {@code true} if running enterprise and corresponding validators need to be included
     * @return list of fully qualified class names, never {@code null}.
     */
    public static List<String> getImplementations(boolean includePro){
        return Arrays.asList(s_implementations).stream().filter(i -> !i.isPro() || includePro)
                .map(Entry::getImplementation)
                .collect(Collectors.toList());
    }

    /**
     * Validates the parts of the deployment relevant for this component.
     * <p>
     * Invoked once on startup, and subsequently on each catalog update.
     *
     * @param catalog the new catalog
     * @param newDep the updated deployment
     * @param curDep current deployment or {@code null} if changes are not to be validated
     * @param ccr the results of validation including any errors need to be set on this result object
     * @return boolean indicating if the validation was successful or not.
     */
    public boolean validateDeployment(Catalog catalog, DeploymentType newDep, DeploymentType curDep, CatalogChangeResult ccr) {
        return true;
    }

    /**
     * Validates consistency of the whole configuration, i.e. catalog and deployment.
     * <p>
     * Invoked once on startup, and subsequently on each catalog update.
     *
     * @param catalog the new catalog
     * @param procedureMapper a function mapping a name to a Procedure in the new catalog, or to {@code null} if not found
     * @param deployment the new deployment
     * @param catalogJar the {@link InMemoryJarfile} of the new catalog
     * @param curCatalog the current catalog or {@code null}
     * @param ccr the results of validation including any errors need to be set on this result object
     * @return {@code true} if successful, {@code false} if not and ccr updated with error message
     */
    public boolean validateConfiguration(Catalog catalog, Function<String, Procedure> procedureMapper,
            DeploymentType deployment, InMemoryJarfile catalogJar, Catalog curCatalog, CatalogChangeResult ccr) {
        return true;
    }
}
