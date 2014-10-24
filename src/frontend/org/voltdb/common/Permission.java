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
import java.util.Collection;
import java.util.EnumSet;
import org.voltdb.catalog.Group;

//If you add a permission here add a boolean in spec.txt and update getPermissionSetForGroup method
public enum Permission {
    //These enums maps to specific boolean in spec.txt
    ADHOC,
    ADMIN,           // aliased by SYSPROC
    DEFAULTPROC,
    DEFAULTPROCREAD;

    public static final String toListString() {
        return Arrays.asList(values()).toString();
    }

    /**
     * Get the Permission enum by its name or alias
     */
    public static final Permission valueOfFromAlias(String name) throws IllegalArgumentException {
        try {
            return valueOf(name);
        } catch (IllegalArgumentException e) {
            // Put the aliases here
            if (name.equalsIgnoreCase("SYSPROC")) {
                return ADMIN;
            } else {
                throw e;
            }
        }
    }

    /**
     * This is there so that bools in spec are converted to enums in one place.
     * @param catGroup defined in catalog
     * @return permissions as <code>EnumSet&ltPermission&gt</code>
     */
    public static final EnumSet<Permission> getPermissionSetForGroup(Group catGroup) {
        EnumSet<Permission> perms;
        if (catGroup.getAdmin() || catGroup.getSysproc()) {
            perms = EnumSet.allOf(Permission.class);
        } else {
            perms = EnumSet.noneOf(Permission.class);
            if (catGroup.getAdhoc()) perms.add(Permission.ADHOC);
            if (catGroup.getDefaultproc()) perms.add(Permission.DEFAULTPROC);
            if (catGroup.getDefaultprocread()) perms.add(Permission.DEFAULTPROCREAD);
        }
        return perms;
    }

    /**
     * Construct a permissions set from a collection of aliases.
     * @throws java.lang.IllegalArgumentException If the alias is not valid. The message is the alias name.
     */
    public static final EnumSet<Permission> getPermissionsFromAliases(Collection<String> aliases)
    throws IllegalArgumentException {
        EnumSet<Permission> permissions = EnumSet.noneOf(Permission.class);
        for (String alias : aliases) {
            try {
                final Permission perm = Permission.valueOfFromAlias(alias.trim().toUpperCase());
                if (perm == ADMIN) {
                    permissions = EnumSet.allOf(Permission.class);
                    break;
                } else {
                    permissions.add(perm);
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(alias.trim().toUpperCase());
            }
        }
        return permissions;
    }

    /**
     * Set the boolean flags in the catalog group based on the given permissions.
     * If a flag is set in the catalog group but the corresponding permission is not
     * present in the permissions set, the flag will NOT be flipped.
     *
     * @param group          The catalog group
     * @param permissions    Permissions to set in the group
     */
    public static final void setPermissionsInGroup(Group group, EnumSet<Permission> permissions) {
        for (Permission p : permissions) {
            switch(p) {
            case ADHOC:
                group.setAdhoc(true);
                break;
            case ADMIN:
                group.setAdmin(true);
                group.setSysproc(true);
                break;
            case DEFAULTPROC:
                group.setDefaultproc(true);
                break;
            case DEFAULTPROCREAD:
                group.setDefaultprocread(true);
                break;
            }
        }
    }
}
