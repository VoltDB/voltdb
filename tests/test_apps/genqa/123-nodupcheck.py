import fileinput

metas = [0] * 6
rows = 0
keyCounts = {}
for line in fileinput.input():
    fields = line.split(',')
    if len(fields) != 29:
        print "Row contains %s fields: %s" % (len(fields), line)
    """
    key = (int(fields[3].replace('"', '')) << 40) | (int(fields[3].replace('"', '')) << 32) |(int(fields[2].replace('"', '')) | 0x00000000) 
    if key in keyCounts:
        keyCounts[key] += 1
    else:
        keyCounts[key] = 1
    """
    metas[int( fields[5].replace('"', ''))] += 1
    rows += 1

print "Inserts: %s\nDeletes: %s\nUpdates/before: %s\nUpdates/after: %s" % \
    (metas[1], metas[2], metas[3], metas[4])
print "Metadata total: %s" % rows

"""
dupes = 0
for key in keyCounts:
    if keyCounts[key] > 1:
        # print "%s: %s" % (key, keyCounts[key])
        dupes += 1

print "Dupes: %s" % dupes
"""
"""
print "Dupes: %s" % len(keyCounts)
"""
