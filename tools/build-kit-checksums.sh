#!/bin/sh

echo "CRC checksums:" > checksums.txt
echo "" >> checksums.txt
cksum *.gz >> checksums.txt
echo "" >> checksums.txt
echo "MD5 checksums:" >> checksums.txt
echo "" >> checksums.txt
md5sum *.gz >> checksums.txt
echo "" >> checksums.txt
echo "SHA1 checksums:" >> checksums.txt
echo "" >> checksums.txt
sha1sum *.gz >> checksums.txt
