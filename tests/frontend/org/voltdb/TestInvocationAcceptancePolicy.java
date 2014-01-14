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

import org.junit.Test;
import org.voltdb.catalog.Procedure;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestInvocationAcceptancePolicy {
    private AuthSystem.AuthUser createUser(boolean adhoc, boolean crud, boolean sysproc,
                                           Procedure userProc)
    {
        AuthSystem.AuthUser user = mock(AuthSystem.AuthUser.class);
        when(user.hasAdhocPermission()).thenReturn(adhoc);
        when(user.hasDefaultProcPermission()).thenReturn(crud);
        if (userProc != null) {
            when(user.hasPermission(userProc)).thenReturn(true);
        }
        when(user.hasSystemProcPermission()).thenReturn(sysproc);
        return user;
    }

    @Test
    public void testSysprocUserPermission()
    {
        AuthSystem.AuthUser user = createUser(false, false, true, null);

        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName("@Pause");

        Procedure proc = SystemProcedureCatalog.listing.get("@Pause").asCatalogProcedure();

        InvocationPermissionPolicy policy = new InvocationPermissionPolicy(true);
        assertNull(policy.shouldAccept(user, invocation, proc));

        // A user that doesn't have sysproc permission
        user = createUser(false, false, false, null);
        assertNotNull(policy.shouldAccept(user, invocation, proc));
    }

    @Test
    public void testAdHocUserPermission()
    {
        AuthSystem.AuthUser user = createUser(true, false, false, null);

        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName("@AdHoc_RW_MP");
        invocation.setParams("select * from T;");

        Procedure proc = SystemProcedureCatalog.listing.get("@AdHoc_RW_MP").asCatalogProcedure();

        InvocationPermissionPolicy policy = new InvocationPermissionPolicy(true);
        assertNull(policy.shouldAccept(user, invocation, proc));

        // A user that doesn't have adhoc permission
        user = createUser(false, false, false, null);
        assertNotNull(policy.shouldAccept(user, invocation, proc));
    }

    @Test
    public void testUserPermission()
    {
        Procedure proc = new Procedure();
        proc.setDefaultproc(true);
        AuthSystem.AuthUser user = createUser(false, false, false, proc);

        StoredProcedureInvocation invocation = new StoredProcedureInvocation();
        invocation.setProcName("A.insert");
        invocation.setParams("test");

        InvocationPermissionPolicy policy = new InvocationPermissionPolicy(true);
        assertNull(policy.shouldAccept(user, invocation, proc));

        // A user that doesn't have crud permission
        user = createUser(false, false, false, null);
        assertNotNull(policy.shouldAccept(user, invocation, proc));
    }
}
