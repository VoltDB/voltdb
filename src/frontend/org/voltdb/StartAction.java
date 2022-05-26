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
import java.util.Collection;
import java.util.EnumSet;

public enum StartAction {

    // Actions that can be produced by the mesh prober, but
    // which are no longer permitted as VoltDB command options.
    CREATE("create"),
    RECOVER("recover"),
    SAFE_RECOVER("recover safemode"),
    REJOIN("rejoin"),
    LIVE_REJOIN("live rejoin"),
    JOIN("add"),

    // Actions that can be given as command options to VoltDB.
    // See also commandOptionSet below
    INITIALIZE("initialize"),
    PROBE("probe"),
    GET("get");

    final static EnumSet<StartAction> commandOptionSet =
            EnumSet.of(INITIALIZE, PROBE, GET);

    final static EnumSet<StartAction> enterpriseOnlySet =
            EnumSet.of(JOIN, SAFE_RECOVER);

    final static EnumSet<StartAction> recoverSet =
            EnumSet.of(RECOVER, SAFE_RECOVER);

    final static EnumSet<StartAction> rejoinSet =
            EnumSet.of(REJOIN, LIVE_REJOIN);

    final static EnumSet<StartAction> joinSet =
            EnumSet.of(REJOIN, LIVE_REJOIN, JOIN);

    final static EnumSet<StartAction> requireEmptyDirsSet =
            EnumSet.of(CREATE);

    final String m_verb;

    StartAction(String verb) {
        m_verb = verb;
    }

    public Collection<String> verbs() {
        return Arrays.asList(m_verb.split("\\s+"));
    }

    public boolean isAllowedCommandOption() {
        return commandOptionSet.contains(this);
    }

    public boolean isLegacy() { // inverse of isAllowedCommandOption
        return !commandOptionSet.contains(this);
    }

    public boolean isEnterpriseOnly() {
        return enterpriseOnlySet.contains(this); // TODO: will eventually be unnecessary
    }

    public boolean doesRecover() {
        return recoverSet.contains(this);
    }

    public boolean doesRejoin() {
        return rejoinSet.contains(this);
    }

    public boolean doesJoin() {
        return joinSet.contains(this);
    }

    public boolean doesRequireEmptyDirectories() {
        return requireEmptyDirsSet.contains(this);
    }
}
