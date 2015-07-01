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

#include "SegvException.hpp"

#include <cstdlib>
#include <cstring> // for strcmp
#include <cxxabi.h>
#include <dlfcn.h>
#include <string>


using namespace std;
using namespace voltdb;

/**
 * The following code is for handling signals. A stack trace will be printed
 * when a SIGSEGV is caught.
 *
 * The code is modified based on the original code found at
 * http://tlug.up.ac.za/wiki/index.php/Obtaining_a_stack_trace_in_C_upon_SIGSEGV
 *
 * This source file is used to print out a stack-trace when your program
 * segfaults. It is relatively reliable and spot-on accurate.
 *
 * This code is in the public domain. Use it as you see fit, some credit
 * would be appreciated, but is not a prerequisite for usage. Feedback
 * on it's use would encourage further development and maintenance.
 *
 * Due to a bug in gcc-4.x.x you currently have to compile as C++ if you want
 * demangling to work.
 *
 * Please note that it's been ported into my ULS library, thus the check for
 * HAS_ULSLIB and the use of the sigsegv_outp macro based on that define.
 *
 * Author: Jaco Kroon <jaco@kroon.co.za>
 *
 * Copyright (C) 2005 - 2010 Jaco Kroon
 */

#if defined(REG_RIP)
# define SIGSEGV_STACK_IA64
#elif defined(REG_EIP)
# define SIGSEGV_STACK_X86
#else
# define SIGSEGV_STACK_GENERIC
#endif

SegvException::SegvException(
    string message,
    void *context,
    const char *filename,
    unsigned long lineno
) : FatalException(message, filename, lineno) {
#ifndef SIGSEGV_NOSTACK
#if defined(SIGSEGV_STACK_IA64) || defined(SIGSEGV_STACK_X86)
    ucontext_t *ucontext = (ucontext_t *)context;
    int f = 0;
    Dl_info dlinfo;
    void **bp = 0;
    void *ip = 0;
    std::vector<std::string> traces;

#if defined(SIGSEGV_STACK_IA64)
    ip = (void*)ucontext->uc_mcontext.gregs[REG_RIP];
    bp = (void**)ucontext->uc_mcontext.gregs[REG_RBP];
#elif defined(SIGSEGV_STACK_X86)
    ip = (void*)ucontext->uc_mcontext.gregs[REG_EIP];
    bp = (void**)ucontext->uc_mcontext.gregs[REG_EBP];
#endif

    while(bp && ip) {
        if(!dladdr(ip, &dlinfo))
            break;

        const char *symname = dlinfo.dli_sname;
        char trace[1024];

#ifndef NO_CPP_DEMANGLE
        int status;
        char * tmp = abi::__cxa_demangle(symname, NULL, 0, &status);

        if (status == 0 && tmp)
            symname = tmp;
#endif

        snprintf(trace, 1024, "% 2d: %p <%s+%lu> (%s)\n",
                ++f,
                ip,
                symname,
                (unsigned long)ip - (unsigned long)dlinfo.dli_saddr,
                dlinfo.dli_fname);
        traces.push_back(std::string(trace));

#ifndef NO_CPP_DEMANGLE
        if (tmp)
            free(tmp);
#endif

        if(dlinfo.dli_sname && !strcmp(dlinfo.dli_sname, "main"))
            break;

        ip = bp[1];
        bp = (void**)bp[0];
    }

    if (!bp || !ip) {
        m_traces = traces;
    }
#endif
#endif
}
