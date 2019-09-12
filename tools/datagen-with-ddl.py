#! /usr/bin/python3
import datagen, itertools, re, sys

# constants for regex
RE_WS = re.compile(' +')
RE_NON_WS = re.compile('\\S')
RE_TRAILING_WS = re.compile(' *$')
RE_LEADING_WS = re.compile('^ *')
RE_BLOCK_COMMENT = re.compile('/\*.*\*/', re.S)

def context(content, startpos, endpos = None, width = 16):
    """
    Get context as [startpos - width, endpos + width] from the given content
    """
    len = len(content)
    if endpos is None:
        endpos = startpos
    assert startpos >= 0 and len > startpos
    assert endpos >= 0 and len > endpos
    assert startpos <= endpos
    return content[max(0, startpos - width) : min(len - 1, endpos +  width)]

def strip_line_comment(line):
    """
    Strip SQL line-style comment from current line: ... -- some comments
    TODO: quoted "--" are still treated as comment.
    """
    endpos = line.find('--')
    return re.sub(RE_TRAILING_WS, '', \
                  line[RE_NON_WS.search(line).end(0) - 1 : (endpos if endpos >= 0 else len(line))]) if line != '' \
            else line

def strip_block_comment(lines):
    """
    Strip SQL block-style comment from the lines:
        ... /* some multi-
        line comments
        that can be quite long
        */;
    TODO: quoted /* or */ are still treated as comment.
    """
    return re.sub(RE_BLOCK_COMMENT, '', lines)


def strip(content):
    """
    Strip DDL content from all comments and blank lines.
    Return a list of processed lines.
    """
    return [line for line in map(strip_line_comment, strip_block_comment(''.join(content)).split('\n')) if RE_WS.fullmatch(line) is None]

def compact(lines):
    """
    Compact so that each statement takes a single line.
    Must be done after comments are removed.
    Returns a list of compacted lines
    TODO: semicolumns in quoted strings are treated as end of statement.
    """
    return ';\n'.join([re.sub(RE_LEADING_WS, '', line) for line in ' '.join(lines).split(';')]).split('\n')

def apply_datagen(line, num_inserts):
    """
    Check whether current statement line is CREATE TABLE statement.
    If it is, append with given number of INSERT statements; otherwise return as is.
    Assume that there is no leading ws in the line.
    """
    line_copy = [line]
    l = list(re.split(RE_WS, line.upper()))
    if len(l) > 2 and l[0] == 'CREATE' and l[1] == 'TABLE':
        line_copy.extend(datagen.main(line, num_inserts).split('\n'))
    return(line_copy)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        datagen.failwith("""
                 Usage: data-gen-with-ddl.py example.ddl 50
                 Will generate DDL mixed with INSERT statements, so that each table has 50 tuples in it.
                 """)
    else:
        num = int(sys.argv[2])
        with open(sys.argv[1], 'r') as fd:
            datagen.failwith(
                '\n'.join(list(itertools.chain.from_iterable(
                    [apply_datagen(line, num) for line in compact(strip(fd.readlines()))]))),
                0)
