package iwdemo;

/**
 * Used in reading county attributes.
 *
 * @author bwhite
 */
public class CountyAttributeBean {
    private String SHAPEID;
    private String SHAPENUMBER;
    private String SHAPECOMPONENT;
    private Integer STATEFP;
    private Integer COUNTYFP;
    private Integer COUNTYNS;
    private String AFFGEOID;
    private String GEOID;
    private String  NAME;
    private String LSAD;
    private String ALAND;
    private String AWATER;

    public final String getSHAPEID() {
        return SHAPEID;
    }
    /**
     * This sets both the SHAPEID integer field and the
     * SHAPENUMBER integer field.
     *
     * @param sHAPEID
     */
    public final void setSHAPEID(String SHAPEID) {
        this.SHAPEID = SHAPEID;
        this.SHAPENUMBER = GeoUtils.getShapeNumber(SHAPEID);
        this.SHAPECOMPONENT = GeoUtils.getShapeComponent(SHAPEID);
    }
    public final String getSHAPENUMBER() {
        return SHAPENUMBER;
    }
    public final String getSHAPECOMPONENT() {
        return SHAPECOMPONENT;
    }
    public final Integer getCOUNTYFP() {
        return COUNTYFP;
    }
    public final void setCOUNTYFP(Integer cOUNTYFP) {
        COUNTYFP = cOUNTYFP;
    }
    public final Integer getCOUNTYNS() {
        return COUNTYNS;
    }
    public final void setCOUNTYNS(Integer cOUNTYNS) {
        COUNTYNS = cOUNTYNS;
    }
    public final String getAFFGEOID() {
        return AFFGEOID;
    }
    public final void setAFFGEOID(String aFFGEOID) {
        AFFGEOID = aFFGEOID;
    }
    public final String getGEOID() {
        return GEOID;
    }
    public final void setGEOID(String gEOID) {
        GEOID = gEOID;
    }
    public final String getNAME() {
        return NAME;
    }
    public final void setNAME(String nAME) {
        NAME = nAME;
    }
    public final String getLSAD() {
        return LSAD;
    }
    public final void setLSAD(String lSAD) {
        LSAD = lSAD;
    }
    public final String getALAND() {
        return ALAND;
    }
    public final void setALAND(String aLAND) {
        ALAND = aLAND;
    }
    public final String getAWATER() {
        return AWATER;
    }
    public final void setAWATER(String aWATER) {
        AWATER = aWATER;
    }
    public final Integer getSTATEFP() {
        return STATEFP;
    }
    public final void setSTATEFP(Integer sTATEFP) {
        STATEFP = sTATEFP;
    }
    public final long getCOUNTYID() {
        return STATEFP * 1000 + COUNTYFP;
    }
}
