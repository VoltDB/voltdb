import fileinput

metas = [0] * 6
rows = 0
keyCounts = {}
for line in fileinput.input():
    fields = line.split(',')
    if len(fields) != 29:
        print "Row contains %s fields: %s" % (len(fields), line)
    metas[int( fields[5].replace('"', ''))] += 1
    rows += 1

print "Inserts: %s\nDeletes: %s\nUpdates/before: %s\nUpdates/after: %s" % \
    (metas[1], metas[2], metas[3], metas[4])
print "Metadata total: %s" % rows
