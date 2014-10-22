/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import org.junit.Test;
import org.voltdb.catalog.Procedure;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.voltdb.InvocationPermissionPolicy.PolicyResult;
import org.voltdb.common.Permission;

public class TestInvocationAcceptancePolicy {
    private AuthSystem.AuthUser createUser(boolean adhoc, boolean crud, boolean sysproc,
                                           Procedure userProc, boolean readonly)
    {

        AuthSystem.AuthUser user = mock(AuthSystem.AuthUser.class);
        when(user.hasPermission(Permission.SYSPROC)).thenReturn(sysproc);
        when(user.hasPermission(Permission.ADHOC, Permission.SYSPROC)).thenReturn(adhoc);
        when(user.hasPermission(Permission.DEFAULTPROC)).thenReturn(crud);
        when(user.hasPermission(Permission.DEFAULTPROCREAD)).thenReturn(readonly);
        if (userProc != null) {
            when(user.hasUserDefinedProcedurePermission(userProc)).thenReturn(true);
        }
        return user;
    }

    @Test
    public void testSysprocUserPermission()
    {
        AuthSystem.AuthUser user = createUser(false, false, true, null, true);

        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName("@Pause");

        Procedure proc = SystemProcedureCatalog.listing.get("@Pause").asCatalogProcedure();

        InvocationPermissionPolicy policy = new InvocationSysprocPermissionPolicy();
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.ALLOW);

        // A user that doesn't have sysproc permission
        user = createUser(false, false, false, null, true);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.DENY);
    }

    @Test
    public void testAdHocUserPermission()
    {
        AuthSystem.AuthUser user = createUser(true, false, false, null, true);

        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName("@AdHoc_RW_MP");
        invocation.setParams("select * from T;");

        Procedure proc = SystemProcedureCatalog.listing.get("@AdHoc_RW_MP").asCatalogProcedure();

        InvocationPermissionPolicy policy = new InvocationAdHocPermissionPolicy();
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.ALLOW);

        // A user that doesn't have adhoc permission
        user = createUser(false, false, false, null, true);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.DENY);
    }

    @Test
    public void testUserDefinedProcPermission()
    {
        Procedure proc = new Procedure();
        AuthSystem.AuthUser user = createUser(false, false, false, proc, true);

        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName("MyProc");
        invocation.setParams("test");

        InvocationPermissionPolicy policy = new InvocationUserDefinedProcedurePermissionPolicy();
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.ALLOW);

    }

    @Test
    public void testUserPermission()
    {
        Procedure proc = new Procedure();
        proc.setDefaultproc(true);
        AuthSystem.AuthUser user = createUser(false, true, false, proc, true);

        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName("A.insert");
        invocation.setParams("test");

        InvocationPermissionPolicy policy = new InvocationDefaultProcPermissionPolicy();
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.ALLOW);

        // A user that doesn't have crud permission
        user = createUser(false, false, false, null, true);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.DENY);
    }

    @Test
    public void testUserPermissionReadOnly()
    {
        Procedure proc = new Procedure();
        proc.setDefaultproc(true);
        proc.setReadonly(true);
        AuthSystem.AuthUser user = createUser(false, false, false, proc, true);

        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName("X.select");
        invocation.setParams("test");

        InvocationPermissionPolicy policy = new InvocationDefaultProcPermissionPolicy();
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.ALLOW);

        // A user that doesn't have crud permission
        Procedure procw = new Procedure();
        procw.setDefaultproc(true);
        procw.setReadonly(false);
        user = createUser(false, false, false, null, false);
        assertEquals(policy.shouldAccept(user, invocation, proc), PolicyResult.DENY);
    }

}
