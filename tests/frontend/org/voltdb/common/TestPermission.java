/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.common;

import com.google_voltpatches.common.collect.Lists;
import org.junit.Test;
import org.voltdb.catalog.Group;

import java.util.Arrays;
import java.util.EnumSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestPermission {
    @Test
    public void testPermissionFromAlias()
    {
        assertEquals(Permission.SQL,           Permission.valueOfFromAlias("SQL"));
        assertEquals(Permission.DEFAULTPROC,     Permission.valueOfFromAlias("DEFAULTPROC"));
        assertEquals(Permission.DEFAULTPROCREAD, Permission.valueOfFromAlias("DEFAULTPROCREAD"));
        assertEquals(Permission.ADMIN,           Permission.valueOfFromAlias("ADMIN"));
        assertEquals(Permission.ADMIN,           Permission.valueOfFromAlias("SYSPROC")); // alias of admin
    }

    @Test
    public void testPermissionsFromAliases()
    {
        verify(Permission.getPermissionsFromAliases(Lists.<String>newArrayList()),
               Permission.getPermissionsFromAliases(Arrays.asList("SQL")),
               Permission.getPermissionsFromAliases(Arrays.asList("SQLREAD")),
               Permission.getPermissionsFromAliases(Arrays.asList("DEFAULTPROC")),
               Permission.getPermissionsFromAliases(Arrays.asList("SYSPROC")),
               Permission.getPermissionsFromAliases(Arrays.asList("ADMIN")),
               Permission.getPermissionsFromAliases(Arrays.asList("SQL", "DEFAULTPROC", "DEFAULTPROCREAD")),
               Permission.getPermissionsFromAliases(Arrays.asList("SQL", "DEFAULTPROC", "ADMIN")));
    }

    @Test
    public void testPermissionsFromGroup()
    {
        Group group = new Group();
        final EnumSet<Permission> none = Permission.getPermissionSetForGroup(group);

        group = new Group();
        group.setSql(true);
        final EnumSet<Permission> sql = Permission.getPermissionSetForGroup(group);

        group = new Group();
        group.setSqlread(true);
        final EnumSet<Permission> sqlread = Permission.getPermissionSetForGroup(group);

        group = new Group();
        group.setDefaultproc(true);
        final EnumSet<Permission> defaultproc = Permission.getPermissionSetForGroup(group);

        group = new Group();
        group.setAdmin(true);
        final EnumSet<Permission> admin = Permission.getPermissionSetForGroup(group);

        group = new Group();
        group.setSql(true);
        group.setDefaultprocread(true);
        group.setDefaultproc(true);
        final EnumSet<Permission> allthree = Permission.getPermissionSetForGroup(group);

        group = new Group();
        group.setSql(true);
        group.setDefaultproc(true);
        group.setAdmin(true);
        final EnumSet<Permission> mixed = Permission.getPermissionSetForGroup(group);

        verify(none, sql, sqlread, defaultproc, admin, admin, allthree, mixed);
    }

    private void verify(EnumSet<Permission> none,
                        EnumSet<Permission> sql,
                        EnumSet<Permission> sqlread,
                        EnumSet<Permission> defaultproc,
                        EnumSet<Permission> sysproc,
                        EnumSet<Permission> admin,
                        EnumSet<Permission> allthree,
                        EnumSet<Permission> mixed)
    {
        assertEquals(EnumSet.noneOf(Permission.class), none);
        assertEquals(EnumSet.of(Permission.SQL, Permission.SQLREAD, Permission.DEFAULTPROC, Permission.DEFAULTPROCREAD), sql);
        assertEquals(EnumSet.of(Permission.SQLREAD, Permission.DEFAULTPROCREAD), sqlread);
        assertEquals(EnumSet.of(Permission.DEFAULTPROC, Permission.DEFAULTPROCREAD), defaultproc);
        assertEquals(EnumSet.allOf(Permission.class),  sysproc);
        assertEquals(EnumSet.allOf(Permission.class),  admin);

        assertEquals(EnumSet.of(Permission.SQL, Permission.SQLREAD, Permission.DEFAULTPROC, Permission.DEFAULTPROCREAD), allthree);
        assertEquals(EnumSet.allOf(Permission.class), mixed);
    }

    @Test
    public void testSetGroup()
    {
        Group group = new Group();
        Permission.setPermissionsInGroup(group, EnumSet.noneOf(Permission.class));
        assertFalse(group.getSql());
        assertFalse(group.getAdmin());
        assertFalse(group.getDefaultproc());
        assertFalse(group.getDefaultprocread());

        group = new Group();
        Permission.setPermissionsInGroup(group, EnumSet.of(Permission.SQL));
        assertTrue(group.getSql());
        assertFalse(group.getAdmin());
        assertFalse(group.getDefaultproc());
        assertFalse(group.getDefaultprocread());

        group = new Group();
        Permission.setPermissionsInGroup(group, EnumSet.of(Permission.ADMIN));
        assertFalse(group.getSql());
        assertTrue(group.getAdmin());
        assertFalse(group.getDefaultproc());
        assertFalse(group.getDefaultprocread());

        group = new Group();
        Permission.setPermissionsInGroup(group, EnumSet.of(Permission.SQL, Permission.DEFAULTPROC, Permission.DEFAULTPROCREAD));
        assertTrue(group.getSql());
        assertFalse(group.getAdmin());
        assertTrue(group.getDefaultproc());
        assertTrue(group.getDefaultprocread());

        group = new Group();
        Permission.setPermissionsInGroup(group, EnumSet.of(Permission.SQL, Permission.ADMIN));
        assertTrue(group.getSql());
        assertTrue(group.getAdmin());
        assertFalse(group.getDefaultproc());
        assertFalse(group.getDefaultprocread());
    }
}
