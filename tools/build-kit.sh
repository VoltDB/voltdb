mkdir -p ~/releases/`cat eng/version.txt`/other
cd eng
svn status
ant clean default dist voltbin
cp obj/release/voltdb-`cat version.txt`.sym ~/releases/`cat version.txt`/other/
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
cksum ~/releases/`cat ../eng/version.txt`/*.* > ~/releases/`cat ../eng/version.txt`/checksums.txt
scp -r ~/releases/`cat ../eng/version.txt` root@community.voltdb.com:/var/www/drupal/sites/default/files/archive
