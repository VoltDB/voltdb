/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.voltdb.InvocationPermissionPolicy.PolicyResult;
import org.voltdb.catalog.Procedure;
import org.voltdb.common.Permission;

public class TestInvocationAcceptancePolicy {
    private AuthSystem.AuthUser createUser(boolean adhoc, boolean crud, boolean sysproc, Procedure userProc,
            boolean readonly, boolean readonlysql, boolean allprocs, boolean compoundprocs)
    {

        AuthSystem.AuthUser user = mock(AuthSystem.AuthUser.class);
        when(user.hasPermission(Permission.SQL)).thenReturn(adhoc);
        when(user.hasPermission(Permission.SQLREAD)).thenReturn(readonlysql);
        when(user.hasPermission(Permission.ADMIN)).thenReturn(sysproc);
        when(user.hasPermission(Permission.DEFAULTPROC)).thenReturn(crud);
        when(user.hasPermission(Permission.DEFAULTPROCREAD)).thenReturn(readonly);
        when(user.hasPermission(Permission.COMPOUNDPROC)).thenReturn(compoundprocs);
        if (userProc != null) {
            // compound procs implies all procs
            when(user.hasUserDefinedProcedurePermission(userProc)).thenReturn(allprocs || compoundprocs);
        }
        return user;
    }

    @Test
    public void testSysprocUserPermission()
    {
        AuthSystem.AuthUser user = createUser(false, false, true, null, true, false, false, false);

        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName("@Pause");

        Procedure proc = SystemProcedureCatalog.listing.get("@Pause").asCatalogProcedure();

        InvocationPermissionPolicy policy = new InvocationSysprocPermissionPolicy();
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.ALLOW);

        // A user that doesn't have admin permission
        user = createUser(false, false, false, null, true, false, false, false);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.DENY);
    }

    @Test
    public void testAdHocUserPermission()
    {
        AuthSystem.AuthUser user = createUser(true, false, false, null, true, false, false, false);

        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName("@AdHoc_RW_MP");
        invocation.setParams("insert into T values (1);");

        Procedure proc = SystemProcedureCatalog.listing.get("@AdHoc_RW_MP").asCatalogProcedure();

        InvocationPermissionPolicy policy = new InvocationSqlPermissionPolicy();
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.ALLOW);

        // Users that doesn't have adhoc permission
        user = createUser(false, false, false, null, true, true, true, false);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.DENY);

        user = createUser(false, false, false, null, true, true, true, true);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.DENY);
    }

    @Test
    public void testAdHocReadUserPermission()
    {
        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName("@AdHoc_RO_MP");
        invocation.setParams("select * from T;");

        Procedure proc = SystemProcedureCatalog.listing.get("@AdHoc_RO_MP").asCatalogProcedure();

        InvocationPermissionPolicy policy = new InvocationSqlPermissionPolicy();

        AuthSystem.AuthUser user = createUser(false, false, false, null, true, true, true, false);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.ALLOW);

        user = createUser(false, false, false, null, true, true, true, true);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.ALLOW);

        // A user that doesn't have adhoc permission
        user = createUser(false, false, false, null, true, false, false, false);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.DENY);
    }

    @Test
    public void testUserDefinedProcPermission()
    {
        Procedure proc = new Procedure();

        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName("MyProc");
        invocation.setParams("test");

        InvocationPermissionPolicy policy = new InvocationUserDefinedProcedurePermissionPolicy();

        // WITH allproc or compoundproc access
        AuthSystem.AuthUser user = createUser(false, false, false, proc, true, true, true, false);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.ALLOW);

        user = createUser(false, false, false, proc, true, true, true, true);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.ALLOW);

        user = createUser(false, false, false, proc, true, true, false, true);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.ALLOW);

        // Without allproc
        user = createUser(false, false, false, proc, false, false, false, false);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.DENY);
        //We cant test individual authorized proc here.
    }

    @Test
    public void testCompoundProcPermission()
    {
        Procedure proc = new Procedure();
        proc.setHasjava(true);  // See CatalogUtil.isCompoundProcedure

        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName("MyProc");
        invocation.setParams("test");

        InvocationPermissionPolicy policy = new InvocationCompoundProcedurePermissionPolicy();

        // WITH allproc or compoundproc access
        AuthSystem.AuthUser user = createUser(false, false, false, proc, true, true, true, false);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.DENY);

        user = createUser(false, false, false, proc, true, true, true, true);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.ALLOW);

        user = createUser(false, false, false, proc, true, true, false, true);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.ALLOW);

        // Without allproc
        user = createUser(false, false, false, proc, false, false, false, false);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.DENY);
        //We cant test individual authorized proc here.
    }

    @Test
    public void testUserPermission()
    {
        Procedure proc = new Procedure();
        proc.setDefaultproc(true);
        AuthSystem.AuthUser user = createUser(false, true, false, proc, true, false, false, false);

        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName("A.insert");
        invocation.setParams("test");

        InvocationPermissionPolicy policy = new InvocationDefaultProcPermissionPolicy();
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.ALLOW);

        // A user that doesn't have crud permission
        user = createUser(false, false, false, null, true, false, false, false);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.DENY);
    }

    @Test
    public void testUserPermissionReadOnly()
    {
        Procedure proc = new Procedure();
        proc.setDefaultproc(true);
        proc.setReadonly(true);
        AuthSystem.AuthUser user = createUser(false, false, false, proc, true, false, false, false);

        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName("X.select");
        invocation.setParams("test");

        InvocationPermissionPolicy policy = new InvocationDefaultProcPermissionPolicy();
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.ALLOW);

        // A user that doesn't have crud permission
        Procedure procw = new Procedure();
        procw.setDefaultproc(true);
        procw.setReadonly(false);
        user = createUser(false, false, false, null, false, false, false, false);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.DENY);
    }

}
