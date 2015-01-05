#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2015 VoltDB Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.

"""
VoltDB catalog code generator.
"""

from catalog_utils import *
from string import Template
from subprocess import Popen

#
# Code generation (shared).
#

def writer( f ):
    def write( *args ):
        f.write( ' '.join( map( str, args ) ) + '\n' )
    return write

def interp(text, params = locals()):
    t = Template(text)
    #return t.safe_substitute(params)
    return t.substitute(params)

#
# Java code generation.
#

def javatypify( x ):
    if x == 'string': return 'String'
    elif x == 'int': return 'int'
    elif x == 'bool': return 'boolean'
    elif x[-1] == '*': return 'CatalogMap<%s>' % x.rstrip('*')
    elif x[-1] == '?': return x.rstrip('?')
    else: raise Exception( 'bad type: ' + x )

def javaobjectify( x ):
    if x == 'string': return 'String'
    elif x == 'int': return 'Integer'
    elif x == 'bool': return 'Boolean'
    elif x[-1] == '*': return 'CatalogMap<%s>' % x.rstrip('*')
    elif x[-1] == '?': return x.rstrip('?')
    else: raise Exception( 'bad type: ' + x )

def genjava( classes, prepath, postpath, package ):
    ##########
    # SETUP
    ##########
    pkgdir = package.replace('.', '/')
    os.system( interp( "rm -rf $postpath/*", locals() ) )
    os.system( interp( "mkdir -p $postpath/", locals() ) )
    os.system( interp( "cp $prepath/Catalog.java $postpath", locals() ) )
    os.system( interp( "cp $prepath/CatalogType.java $postpath", locals() ) )
    os.system( interp( "cp $prepath/CatalogMap.java $postpath", locals() ) )
    os.system( interp( "cp $prepath/CatalogException.java $postpath", locals() ) )
    os.system( interp( "cp $prepath/CatalogChangeGroup.java $postpath", locals() ) )
    os.system( interp( "cp $prepath/CatalogDiffEngine.java $postpath", locals() ) )
    os.system( interp( "cp $prepath/FilteredCatalogDiffEngine.java $postpath", locals() ) )

    ##########
    # WRITE THE SOURCE FILES
    ##########

    for cls in classes:
        clsname = cls.name
        javapath = postpath + "/" + clsname + '.java'
        #ensure_relative_path_exists(postpath + "/" + pkgdir)
        f = file( javapath, 'w' )
        if not f:
            raise OSError("Can't create file %s for writing" % javapath)
        write = writer( f )
        write (gpl_header)
        write (auto_gen_warning)
        write('package', package + ';\n')

        if cls.has_comment():
            write('/**\n *', cls.comment)
            write(' */')

        write( interp( 'public class $clsname extends CatalogType {\n', locals() ) )

        # fields
        for field in cls.fields:
            ftype = javatypify( field.type )
            fname = field.name
            #realtype = field.type[:-1]
            #methname = fname.capitalize()
            if ftype == "String":
                write( interp( '    String m_$fname = new String();', locals() ) )
            elif field.type[-1] == '?':
                pass # don't keep local cached vars for references
            else:
                write( interp( '    $ftype m_$fname;', locals() ) )
        write( '' )

        # setBaseValues
        write( '    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {' )
        write( '        super.setBaseValues(catalog, parent, path, name);')
        for field in cls.fields:
            ftype = javatypify( field.type )
            fname = field.name
            realtype = field.type[:-1]
            #methname = fname.capitalize()
            if field.type[-1] == '*':
                write( interp( '        m_$fname = new $ftype(catalog, this, path + "/" + "$fname", $realtype.class);', locals() ) )
                write( interp( '        m_childCollections.put("$fname", m_$fname);', locals() ) )
            elif field.type[-1] == '?':
                write( interp( '        m_fields.put("$fname", null);', locals() ) )
            else:
                write( interp( '        m_fields.put("$fname", m_$fname);', locals() ) )
        write( '    }\n' )

        # update
        write ( '    void update() {' )
        for field in cls.fields:
            ftype = javatypify( field.type )
            fobjtype = javaobjectify( field.type )
            fname = field.name
            realtype = field.type[:-1]
            methname = fname.capitalize()
            if field.type[-1] == '?':
                pass # don't keep local cached vars for references
            elif field.type[-1] != '*':
                write( interp( '        m_$fname = ($fobjtype) m_fields.get("$fname");', locals() ) )
        write( '    }\n' )

        # getter methods
        for field in cls.fields:
            ftype = javatypify( field.type )
            fname = field.name
            realtype = field.type[:-1]
            methname = fname.capitalize()
            if field.has_comment():
                write('    /** GETTER:', field.comment, '*/')
            write( interp( '    public $ftype get$methname() {', locals() ) )
            if field.type[-1] == '?':
                write( interp( '        Object o = getField("$fname");', locals() ) )
                write( interp( '        if (o instanceof UnresolvedInfo) {', locals() ) )
                write( interp( '            UnresolvedInfo ui = (UnresolvedInfo) o;', locals() ) )
                write( interp( '            $ftype retval = ($ftype) m_catalog.getItemForRef(ui.path);', locals() ) )
                write( interp( '            assert(retval != null);', locals() ) )
                write( interp( '            m_fields.put("$fname", retval);', locals() ) )
                write( interp( '            return retval;', locals() ) )
                write( interp( '        }', locals() ) )
                write( interp( '        return ($ftype) o;', locals() ) )
            else:
                write( interp( '        return m_$fname;', locals() ) )
            write( '    }\n' )

        # setter methods
        for field in cls.fields:
            ftype = javatypify( field.type )
            fname = field.name
            realtype = field.type[:-1]
            methname = fname.capitalize()
            if field.type[-1] == '*':
                continue
            if field.has_comment():
                write('    /** SETTER:', field.comment, '*/')
            write( interp( '    public void set$methname($ftype value) {', locals() ) )
            if field.type[-1] == '?':
                write( interp( '        m_fields.put("$fname", value);', locals() ) )
            else:
                write( interp( '        m_$fname = value; m_fields.put("$fname", value);', locals() ) )
            write( '    }\n' )

        # wrap up
        write( '}' )

#
# C++ code generation.
#

def cpptypify( x ):
    if x == 'string': return 'std::string'
    elif x == 'int': return 'int32_t'
    elif x == 'bool': return 'bool'
    elif x[-1] == '*': return 'CatalogMap<%s>' % x.rstrip('*')
    elif x[-1] == '?': return 'CatalogType*'
    else: raise Exception( 'bad type: ' + x )

def gencpp( classes, prepath, postpath ):
    ##########
    # SETUP
    ##########
    os.system( interp( "rm -rf $postpath/*", locals() ) )
    os.system( interp( "mkdir -p $postpath/", locals() ) )
    os.system( interp( "cp $prepath/catalog.h $postpath", locals() ) )
    os.system( interp( "cp $prepath/catalogtype.h $postpath", locals() ) )
    os.system( interp( "cp $prepath/catalogmap.h $postpath", locals() ) )
    os.system( interp( "cp $prepath/catalog.cpp $postpath", locals() ) )
    os.system( interp( "cp $prepath/catalogtype.cpp $postpath", locals() ) )

    ##########
    # WRITE THE SOURCE FILES
    ##########
    for cls in reversed( classes ):
        clsname = cls.name

        referencedClasses = []
        for field in cls.fields:
            classType = field.type[:-1]
            if (field.type[-1] == "*") or (field.type[-1] == '?'):
                if classType not in referencedClasses:
                    referencedClasses.append(classType)
        if cls.name in referencedClasses:
            referencedClasses.remove(cls.name)

        ##########
        # WRITE THE HEADER FILE
        ##########

        f = file( postpath + "/" + clsname.lower() + ".h", 'w' )
        write = writer( f )

        write (gpl_header)
        write (auto_gen_warning)
        pp_unique_str = "CATALOG_" + clsname.upper() + "_H_"
        write("#ifndef", pp_unique_str);
        write("#define", pp_unique_str);
        write("")

        write('#include <string>')
        write('#include "catalogtype.h"')
        write('#include "catalogmap.h"\n')
        write('namespace catalog {\n')

        for classType in referencedClasses:
            write("class " + classType + ";")

        if cls.has_comment():
            write('/**\n *', cls.comment)
            write(' */')

        write( interp( 'class $clsname : public CatalogType {', locals() ) )
        write( '    friend class Catalog;' )
        write( interp( '    friend class CatalogMap<$clsname>;', locals() ) )

        # protected section
        write( '\nprotected:')

        # constructor
        write( '    ' + clsname + '(Catalog * catalog, CatalogType * parent, const std::string &path, const std::string &name);' )

        # Field Member variables.
        for field in cls.fields:
            ftype = cpptypify( field.type )
            privname = 'm_' + field.name
            write( interp( '    $ftype $privname;', locals() ) )

        # update method
        write("\n    virtual void update();\n")

        # add method
        write("    virtual CatalogType * addChild(const std::string &collectionName, const std::string &name);")

        # getChild method
        write("    virtual CatalogType * getChild(const std::string &collectionName, const std::string &childName) const;")

        # removeChild method
        write("    virtual bool removeChild(const std::string &collectionName, const std::string &childName);")

        # public section
        write("\npublic:")

        # destructor
        write('    ~' + clsname + '();\n');

        # getter methods
        for field in cls.fields:
            ftype = cpptypify( field.type )
            privname = 'm_' + field.name
            pubname = field.name
            Pubname = pubname.capitalize()
            if field.has_comment():
                write('    /** GETTER:', field.comment, '*/')
            if field.type == 'string':
                write ( interp( '    const std::string & $pubname() const;', locals() ) )
            elif field.type[-1] == '?':
                write ( "    const " + field.type[:-1] + " * " + pubname + "() const;")
            elif field.type[-1] == '*':
                write ( interp( '    const $ftype & $pubname() const;', locals() ) )
            else:
                write ( interp( '    $ftype $pubname() const;', locals() ) )

        write( '};\n' )

        write( '} // namespace catalog\n' )

        write ("#endif // ", pp_unique_str)

        ##########
        # WRITE THE CPP FILE
        ##########

        f = file( postpath + "/" + clsname.lower() + ".cpp", 'w' )
        write = writer( f )

        write (gpl_header)
        write (auto_gen_warning)
        filename = clsname.lower()
        write ( '#include <cassert>' )
        write ( interp( '#include "$filename.h"', locals() ) )
        write ( '#include "catalog.h"' )
        otherhdrs = ['#include "%s.h"' % field.type[:-1].lower() for field in cls.fields if field.type[-1] in ['*', '?'] ]
        uniques = {}
        for hdr in otherhdrs:
            uniques[hdr] = hdr
        for hdr in uniques.keys():
            write( hdr )
        write ( '\nusing namespace catalog;' )
        write ( 'using namespace std;\n' )

        # write the constructor
        mapcons = ["m_%s(catalog)" % field.name for field in cls.fields if field.type[-1] == '*']
        write ( interp( '$clsname::$clsname(Catalog *catalog, CatalogType *parent, const string &path, const string &name)', locals() ) )
        comma = ''
        if len(mapcons): comma = ','
        write ( interp( ': CatalogType(catalog, parent, path, name)$comma', locals()))

        mapcons = ["m_%s(catalog, this, path + \"/\" + \"%s\")" % (field.name, field.name) for field in cls.fields if field.type[-1] == '*']
        if len(mapcons) > 0:
            write( "  " + ", ".join(mapcons))
        write('{')

        # init the fields and childCollections
        write( '    CatalogValue value;' )
        for field in cls.fields:
            ftype = cpptypify( field.type )
            privname = 'm_' + field.name
            pubname = field.name
            if field.type[-1] == '*':
                write( interp( '    m_childCollections["$pubname"] = &$privname;', locals() ) )
            else:
                write( interp( '    m_fields["$pubname"] = value;', locals() ) )
        write ( "}\n" )

        # write the destructor
        write(clsname + '::~' + clsname + '() {');
        for field in cls.fields:
            if field.type[-1] == '*':
                ftype = field.type.rstrip('*')
                itr = ftype.lower() + '_iter'
                privname = 'm_' + field.name
                tab = '   '
                write(interp('$tab std::map<std::string, $ftype*>::const_iterator $itr = $privname.begin();', locals()))
                write(interp('$tab while ($itr != $privname.end()) {', locals()))
                write(interp('$tab $tab delete $itr->second;', locals()))
                write(interp('$tab $tab $itr++;', locals()))
                write(interp('$tab }', locals()))
                write(interp('$tab $privname.clear();\n', locals()))
        write('}\n')

        # write update()
        write ( interp( 'void $clsname::update() {', locals() ) )
        for field in cls.fields:
            ftype = cpptypify( field.type )
            privname = 'm_' + field.name
            pubname = field.name
            if field.type[-1] == '?':
                ext = "typeValue"
                write( interp( '    $privname = m_fields["$pubname"].$ext;', locals() ) )
            elif field.type[-1] != '*':
                ext = "intValue"
                if (ftype == 'std::string'):
                    ext = "strValue.c_str()"
                write( interp( '    $privname = m_fields["$pubname"].$ext;', locals() ) )
        write ( "}\n" )

        # write add(...)
        write ( interp( 'CatalogType * $clsname::addChild(const std::string &collectionName, const std::string &childName) {', locals() ) )
        for field in cls.fields:
            if field.type[-1] == "*":
                privname = 'm_' + field.name
                pubname = field.name
                write ( interp( '    if (collectionName.compare("$pubname") == 0) {', locals() ) )
                write ( interp( '        CatalogType *exists = $privname.get(childName);', locals() ) )
                write ( '        if (exists)\n            return NULL;' )
                write ( interp( '        return $privname.add(childName);\n    }', locals() ) )
        write ( '    return NULL;\n}\n' )

        # write getChild(...)
        write ( interp( 'CatalogType * $clsname::getChild(const std::string &collectionName, const std::string &childName) const {', locals() ) )
        for field in cls.fields:
            if field.type[-1] == "*":
                privname = 'm_' + field.name
                pubname = field.name
                write ( interp( '    if (collectionName.compare("$pubname") == 0)', locals() ) )
                write ( interp( '        return $privname.get(childName);', locals() ) )
        write ( '    return NULL;\n}\n' )

        # write removeChild(...)
        write ( interp( 'bool $clsname::removeChild(const std::string &collectionName, const std::string &childName) {', locals() ) )
        write ( interp( '    assert (m_childCollections.find(collectionName) != m_childCollections.end());', locals() ) )
        for field in cls.fields:
            if field.type[-1] == "*":
                privname = 'm_' + field.name
                pubname = field.name
                write ( interp( '    if (collectionName.compare("$pubname") == 0) {', locals() ) )
                write ( interp( '        return $privname.remove(childName);', locals() ) )
                write ( interp( '    }', locals() ) )
        write ( interp( '    return false;', locals() ) )
        write ( '}\n' )

        # write field getters
        for field in cls.fields:
            ftype = cpptypify( field.type )
            privname = 'm_' + field.name
            pubname = field.name
            outertype = field.type[:-1]
            if field.type == 'string':
                write ( interp( 'const string & $clsname::$pubname() const {\n    return $privname;\n}\n', locals() ) )
            elif field.type[-1] == '?':
                write ( interp( 'const $outertype * $clsname::$pubname() const {', locals() ) )
                write ( interp( '    return dynamic_cast<$outertype*>($privname);\n}\n', locals() ) )
            elif field.type[-1] == '*':
                write ( interp( 'const $ftype & $clsname::$pubname() const {\n    return $privname;\n}\n', locals() ) )
            else:
                write ( interp( '$ftype $clsname::$pubname() const {\n    return $privname;\n}\n', locals() ) )

#
# Main.
#

def main():
    specpath = "spec.txt"
    javapkg = 'org.voltdb.catalog'
    cpp_postpath = 'out/cppsrc'
    cpp_prepath = 'in/cppsrc'
    java_prepath = 'in/javasrc'
    java_postpath = 'out/javasrc'
    f =  file( specpath )
    classes = parse( f.read() )
    genjava( classes, java_prepath, java_postpath, javapkg )
    gencpp( classes, cpp_prepath, cpp_postpath )

main()
