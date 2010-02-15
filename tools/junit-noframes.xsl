<?xml version='1.0' encoding='UTF-8'?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:output method="text"/>

<!-- find failing testsuites -->

<xsl:template match="/">
<xsl:text>See obj/release/testoutput/report/junit-noframes.html for details.
</xsl:text>
  <xsl:choose>
    <xsl:when test="testsuites/testsuite[@failures != 0]">
      <xsl:apply-templates select="testsuites/testsuite[@failures != 0]"/>
    </xsl:when>
    <xsl:otherwise>
      <xsl:value-of select="concat('All tests passed', '.')"/>
    </xsl:otherwise>
  </xsl:choose>
<xsl:text>
</xsl:text>
</xsl:template> 

<xsl:template match="testsuite">
  <xsl:value-of select="concat('Suite ', @name, ' reports ', @failures, ' failure(s).')"/>
<xsl:text>
</xsl:text>
  <xsl:apply-templates select="testcase[failure]"/>
</xsl:template>

<xsl:template match="testcase">
  <xsl:value-of select="concat('  Testcase ', @name, ' ', failure/@type)"/>
<xsl:text>
</xsl:text>
</xsl:template> 

</xsl:stylesheet>
