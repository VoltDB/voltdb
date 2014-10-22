/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb.common;

import java.util.Arrays;
import java.util.EnumSet;
import org.voltdb.catalog.Group;

//If you add a permission here add a boolean in spec.txt and update getPermissionSetForGroup method
public enum Permission {
    //These enums maps to specific boolean in spec.txt
    ADHOC,
    SYSPROC,
    DEFAULTPROC,
    DEFAULTPROCREAD;

    public static final String toListString() {
        return Arrays.asList(values()).toString();
    }

    /**
     * This is there so that bools in spec are converted to enums in one place.
     * @param catGroup defined in catalog
     * @return permissions as <code>EnumSet&ltPermission&gt</code>
     */
    public static final EnumSet<Permission> getPermissionSetForGroup(Group catGroup) {
        EnumSet<Permission> perms = EnumSet.noneOf(Permission.class);
        if (catGroup.getAdhoc()) perms.add(Permission.ADHOC);
        if (catGroup.getSysproc()) perms.add(Permission.SYSPROC);
        if (catGroup.getDefaultproc()) perms.add(Permission.DEFAULTPROC);
        if (catGroup.getDefaultprocread()) perms.add(Permission.DEFAULTPROCREAD);
        return perms;
    }


}
