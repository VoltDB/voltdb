/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 VoltDB Inc.
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
import java.util.Map;
import java.util.regex.Pattern;

import com.google_voltpatches.common.collect.ImmutableMap;

public enum StartAction {

    // Actions that can be produced by the mesh prober, but
    // which are no longer permitted as VoltDB command options.
    CREATE("create", false),
    RECOVER("recover", false),
    SAFE_RECOVER("recover safemode", true),
    REJOIN("rejoin", false),
    LIVE_REJOIN("live rejoin", false),
    JOIN("add", true),

    // Actions that can be given as command options to VoltDB.
    // See also commandOptionSet below
    INITIALIZE("initialize", false),
    PROBE("probe", false),
    GET("get", false);

    final static Pattern spaces = Pattern.compile("\\s+");

    final static Map<String, StartAction> verbMoniker;

    final static EnumSet<StartAction> recoverSet =
            EnumSet.of(RECOVER,SAFE_RECOVER);

    final static EnumSet<StartAction> rejoinSet =
            EnumSet.of(REJOIN,LIVE_REJOIN);

    final static EnumSet<StartAction> joinSet =
            EnumSet.of(REJOIN,LIVE_REJOIN,JOIN);

    final static EnumSet<StartAction> requireEmptyDirsSet =
            EnumSet.of(CREATE);

    final static EnumSet<StartAction> commandOptionSet =
            EnumSet.of(INITIALIZE, PROBE, GET);

    final String m_verb;
    final boolean m_enterpriseOnly;

    static {
        ImmutableMap.Builder<String, StartAction> mb = ImmutableMap.builder();
        for (StartAction action: StartAction.values()) {
            mb.put(action.m_verb, action);
        }
        verbMoniker = mb.build();
    }

    StartAction(String verb, boolean enterpriseOnly) {
        m_verb = verb;
        m_enterpriseOnly = enterpriseOnly;
    }

    public static StartAction monickerFor(String verb) {
        if (verb == null) {
            return null;
        }
        verb = spaces.matcher(verb.trim().toLowerCase()).replaceAll(" ");
        return verbMoniker.get(verb); // TODO: used only for validity check on verb, remove?
    }

    public Collection<String> verbs() {
        return Arrays.asList(m_verb.split("\\s+"));
    }

    public boolean isEnterpriseOnly() {
        return m_enterpriseOnly; // TODO: will eventually be unnecessary
    }

    public boolean isAllowedCommandOption() {
        return commandOptionSet.contains(this);
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

    public boolean isLegacy() {
        return !commandOptionSet.contains(this);
    }

    public boolean doesRequireEmptyDirectories() {
        return requireEmptyDirsSet.contains(this);
    }
}
