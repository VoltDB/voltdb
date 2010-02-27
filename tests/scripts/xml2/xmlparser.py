# Copyright (C) 2009 by Ning Shi and Andy Pavlo
# Brown University
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
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import sys
import os
from urlparse import urlparse
from xml.sax import make_parser
from xml.sax.handler import ContentHandler
from xml.sax.handler import feature_namespaces
from xml.sax.xmlreader import InputSource
try:
    from cStringIO import StringIO as _StringIO
except:
    from StringIO import StringIO as _StringIO

# Detects the directory which contains this file so that we can import httplib2.
cwd = os.getcwd()
realpath = os.path.realpath(__file__)
basedir = os.path.dirname(realpath)
basename = os.path.basename(realpath)
if not os.path.exists(realpath):
    if os.path.exists(os.path.join(cwd, basename)):
        basedir = cwd
sys.path.append(os.path.dirname(basedir))
from exceptions import *

class ContentParser(ContentHandler):
    """XML handler class.

    This class is used by the SAX XML parser.
    """

    __parallels = ("Statements",)
    __terminals = ("SQL", "Status", "Info", "Result", "Seed")

    def __init__(self, l):
        """Constructor.

        'l': An empty dictionary to be filled with request pairs.
        """

        ContentHandler.__init__(self)
        self.rp_list = l
        self.__current_key = []
        self.__current = [self.rp_list]

    def startElement(self, name, attrs):
        """Marks the start of an element.

        This is the callback used by the SAX XML parser to notify us that a new
        element begins.

        'name': The name of the element.
        'attrs': The attributes of the element.
        """

        name = name.encode("utf-8")

        if name in self.__terminals:
            self.__current_key.append(name)
            return
        elif name in self.__parallels:
            self.__current[-1][name] = []
            self.__current.append(self.__current[-1][name])
        else:
            if type(self.__current[-1]) is list:
                self.__current[-1].append({})
                self.__current.append(self.__current[-1][-1])
            else:
                self.__current[-1][name] = {}
                self.__current.append(self.__current[-1][name])
        self.__current_key.append(name)

        for n in attrs.getNames():
            self.__current[-1][n.encode("utf-8")] = attrs.getValue(n)

    def endElement(self, name):
        """Marks the end of an element.

        This is the callback used by the SAX XML parser to notify us that an
        opened element ends.

        'name': The name of the element.
        """

        name = name.encode("utf-8")

        if self.__current_key[-1] != name:
            raise InvalidXML("Start tag does not match end tag.")

        if name not in self.__terminals:
            self.__current.pop()
        self.__current_key.pop()

    def characters(self, content):
        """Marks the inner content of an element.

        This is the callback used by the SAX XML parser to notify us about the
        inner content of an element.

        'content': The inner content.
        """

        content = content.strip()

        if content:
            if len(self.__current_key) == 0:
                raise InvalidXML("No tag opened.")

            if type(self.__current[-1]) is list:
                self.__current[-1].append(content)
            else:
                if self.__current_key[-1] in self.__current[-1]:
                    content = self.__current[-1][self.__current_key[-1]] + \
                        content
                else:
                    content = content

                self.__current[-1][self.__current_key[-1]] = content

class XMLParser:
    """The XML parser.

    TODO: We should validate the XML before parsing.
    """

    def __init__(self, url_file_string):
        """Constructor.

        'url_file_string': A URL, a file handle, a filename which points to the
        actual XML document or the actual XML data.
        """

        self.url_file_string = url_file_string
        self.rp_list = {}
        self.__data = None
        self.__parser = make_parser()
        self.__parser.setFeature(feature_namespaces, 0)
        self.__parser.setContentHandler(ContentParser(self.rp_list))

    def __open_resource__(self):
        """Opens the resource depends on the type of information given.

        If it is a file handle, nothing needs to be done; if it is the XML data,
        make it readable like a file; if it is a filename, open it and return
        the file handle.

        Return: A handle to read from by calling the method 'read()' of the
        handle.
        """

        if hasattr(self.url_file_string, 'read'):
            return self.url_file_string

        if self.url_file_string == "-":
            return sys.stdin

        if self.url_file_string[0] == "<":
            return _StringIO(self.url_file_string.encode("utf-8"))

        try:
            return open(self.url_file_string)
        except:
            pass

    def __read_data__(self):
        """Reads the XML document.
        """

        if self.__data:
            return

        fd = self.__open_resource__()
        if fd:
            data = fd.read()
            fd.close()

        self.__data = InputSource()
        self.__data.setByteStream(_StringIO(data))

    def __parse__(self):
        """Parses the XML document.
        """

        if not self.__data:
            self.__read_data__()

        if self.__data:
            self.__parser.parse(self.__data)

    def get_data(self):
        """Gets the request pairs dictionary.

        Return: The request pairs dictionary.
        """

        if not self.__data:
            self.__parse__()

        return self.rp_list["SQLGenerator"]
