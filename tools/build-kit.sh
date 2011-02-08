mkdir -p ~/releases/`cat eng/version.txt`
cd eng
svn status
ant clean default dist voltbin
cp obj/release/voltdb-`cat version.txt`.tar.gz ~/releases/`cat version.txt`/LINUX-voltdb-`cat version.txt`.tar.gz
cd ../pro
svn status
ant -f mmt.xml clean
VOLTCORE=../eng ant -f mmt.xml dist.pro
cp obj/pro/voltdb-ent-* ~/releases/`cat ../eng/version.txt`/
cd ../doc
mv pdfs/enterprise_releasenotes.pdf ~/releases/`cat ../eng/version.txt`/`cat ../eng/version.txt`-enterprise_releasenotes.pdf
zip ~/releases/`cat ../eng/version.txt`/`cat ../eng/version.txt`-docs_pdf.zip pdfs/*.pdf
cp ~/releases/`cat ../eng/version.txt`/`cat ../eng/version.txt`-enterprise_releasenotes.pdf pdfs/enterprise_releasenotes.pdf
cd ../eng
echo "CRC checksums:" > ~/releases/`cat version.txt`/checksums.txt
echo "" >> ~/releases/`cat version.txt`/checksums.txt
cksum ~/releases/`cat version.txt`/*.gz >> ~/releases/`cat version.txt`/checksums.txt
echo "MD5 checksums:" >> ~/releases/`cat version.txt`/checksums.txt
echo "" >> ~/releases/`cat version.txt`/checksums.txt
md5sum ~/releases/`cat version.txt`/*.gz >> ~/releases/`cat version.txt`/checksums.txt
echo "SHA1 checksums:" >> ~/releases/`cat version.txt`/checksums.txt
echo "" >> ~/releases/`cat version.txt`/checksums.txt
sha1sum ~/releases/`cat version.txt`/*.gz >> ~/releases/`cat version.txt`/checksums.txt
scp -r ~/releases/`cat version.txt` root@community.voltdb.com:/var/www/drupal/sites/default/files/archive
mkdir -p ~/releases/`cat version.txt`/other
cp obj/release/voltdb-`cat version.txt`.sym ~/releases/`cat version.txt`/other/
cd ..
