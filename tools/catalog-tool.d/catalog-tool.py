# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import sys
import os
import shutil
import zipfile
import tempfile
import re
from voltcli import environment
from voltcli import utility


reHexPat = '^(\s*set\s+.*\s+)(schema|explainplan|plannodetree)(\s+")([0-9a-f]+)(".*)$'
reHex = re.compile(reHexPat, re.IGNORECASE)


def get_temp_directory(suffix=None):
    return tempfile.mkdtemp(prefix='%s_' % environment.command_name)


def extract_catalog(runner, catalog_old):
    tempdir = get_temp_directory()
    try:
        catalog_zip_in = zipfile.ZipFile(catalog_old, 'r')
    except (IOError, OSError), e:
        runner.abort('Failed to open input catalog jar "%s".' % catalog_old)
    try:
        try:
            catalog_zip_in.extractall(tempdir)
        except (IOError, OSError), e:
            runner.abort('Failed to extract catalog jar "%s".' % catalog_old, e)
    finally:
        catalog_zip_in.close()
    return tempdir


def compress_catalog(runner, catalog_new, tempdir):
    try:
        catalog_zip_out = zipfile.ZipFile(catalog_new, 'w', zipfile.ZIP_DEFLATED)
    except (IOError, OSError), e:
        runner.abort('Failed to open output catalog jar "%s".' % catalog_new)
    try:
        savedir = os.getcwd()
        os.chdir(tempdir)
        try:
            try:
                for root, dirs, files in os.walk('.'):
                    for f in files:
                        catalog_zip_out.write(os.path.join(root, f))
            except (IOError, OSError), e:
                runner.abort('Failed to compress catalog jar "%s".' % catalog_new, e)
        finally:
            catalog_zip_out.close()
    finally:
        os.chdir(savedir)


def patch_buildinfo(runner, buildinfo_in, version_new):
    buildinfo_out = '%s.out' % buildinfo_in
    buildinfo_orig = '%s.orig' % buildinfo_in
    if not os.path.isfile(buildinfo_in):
        runner.abort('"%s" not found.' % buildinfo_in)
    try:
        shutil.copy(buildinfo_in, buildinfo_orig)
    except (IOError, OSError), e:
        runner.abort('Failed to backup "%s" to "%s".'
                        % (buildinfo_in, buildinfo_orig), e)
    try:
        f_in = open(buildinfo_in, 'r')
    except (IOError, OSError), e:
        runner.abort('Failed to open input file "%s".' % buildinfo_in)
    try:
        try:
            f_out = open(buildinfo_out, 'w')
        except (IOError, OSError), e:
            runner.abort('Failed to open output file "%s".' % buildinfo_out)
        try:
            nline = 0
            for line in f_in:
                if nline == 0:
                    f_out.write('%s\n' % version_new)
                else:
                    f_out.write(line)
                nline += 1
        finally:
            f_out.close()
    finally:
        f_in.close()
    try:
        shutil.move(buildinfo_out, buildinfo_in)
    except (IOError, OSError), e:
        runner.abort('Failed to move "%s" to "%s".'
                        % (buildinfo_out, buildinfo_in), e)


def iter_catalog_dump(runner, path, all):
    try:
        zipf = zipfile.ZipFile(path)
        try:
            try:
                yield ''
                yield '========== Build Info =========='
                yield ''
                for line in zipf.read('buildinfo.txt').split('\n'):
                    if line:
                        yield line
                if all:
                    yield ''
                    yield '========== Catalog =========='
                    yield ''
                    for line in zipf.read('catalog.txt').split('\n'):
                        m = reHex.search(line)
                        if m:
                            yield ''.join((m.group(1),
                                           m.group(2),
                                           m.group(3),
                                           m.group(4).decode('hex'),
                                           m.group(5)))
                        else:
                            yield line
            except Exception, e:
                runner.error(e)
        finally:
            zipf.close()
    except (IOError, OSError), e:
        runner.abort('Unable to open catalog "%s".' % path, e)


def dump_to_file(runner, path_in, path_out):
    writer = utility.File(path_out, 'w')
    writer.open()
    try:
        for line in iter_catalog_dump(runner, path_in, True):
            writer.write('%s\n' % line)
    finally:
        writer.close()


@VOLT.Command(
    description='Display catalog information.',
    arguments=[
        VOLT.PathArgument('catalog_path', 'path to catalog jar', exists=True),
    ],
    options=[
        VOLT.BooleanOption('-a', '--a', 'all', 'show all catalog information'),
    ],
)
def dump(runner):
    for line in iter_catalog_dump(runner, runner.opts.catalog_path, runner.opts.all):
        print line


@VOLT.Command(
    description='Patch a catalog file version (buildinfo.txt).',
    description2='This command does not overwrite the original catalog file.',
    arguments=[
        VOLT.PathArgument('catalog_path', 'path to catalog jar', exists=True),
        VOLT.StringArgument('version', 'new version number'),
    ],
)
def patch_version(runner):
    version_new = runner.opts.version
    if not version_new:
        runner.abort('A non-empty version number is required.')
    catalog_old = runner.opts.catalog_path
    basename, extension = os.path.splitext(catalog_old)
    catalog_new = '%(basename)s-%(version_new)s%(extension)s' % locals()
    runner.info('Extracting "%s"...' % catalog_old)
    tempdir = extract_catalog(runner, catalog_old)
    try:
        buildinfo_in = os.path.join(tempdir, 'buildinfo.txt')
        runner.info('Patching version to "%s"...' % version_new)
        patch_buildinfo(runner, buildinfo_in, version_new)
        runner.info('Compressing output file "%s"...' % catalog_new)
        compress_catalog(runner, catalog_new, tempdir)
    finally:
        runner.verbose_info('Removing temporary directory "%s"...' % tempdir)
        shutil.rmtree(tempdir)


@VOLT.Command(
    description='Display the difference between two catalogs.',
    arguments=[
        VOLT.PathArgument('catalog_path_1', 'path to catalog jar 1', exists=True),
        VOLT.PathArgument('catalog_path_2', 'path to catalog jar 2', exists=True),
    ],
    options=[
        VOLT.BooleanOption('-k', '--keep', 'keep', "keep (don't delete) dump files"),
    ],
)
def diff(runner):
    tempdir = get_temp_directory(suffix='_diff')
    try:
        path1 = os.path.join(tempdir, '%s.out' % os.path.basename(runner.opts.catalog_path_1))
        path2 = os.path.join(tempdir, '%s.out' % os.path.basename(runner.opts.catalog_path_2))
        dump_to_file(runner, runner.opts.catalog_path_1, path1)
        dump_to_file(runner, runner.opts.catalog_path_2, path2)
        os.system('diff "%s" "%s"' % (path1, path2))
    finally:
        if not runner.opts.keep:
            shutil.rmtree(tempdir)
        else:
            print '\n'
            runner.info('Dump files can be found in "%s".' % tempdir)
