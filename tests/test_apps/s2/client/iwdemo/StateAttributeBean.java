package iwdemo;

public class StateAttributeBean {
    String SHAPEID;
    Integer STATEFP;
    String STATENS;
    String AFFGEOID;
    String GEOID;
    String STUSPS;
    String NAME;
    String LSAD;
    String ALAND;
    String AWATER;
    public final Long getSTATEID() {
        return STATEFP.longValue();
    }
    public final String getSHAPEID() {
        return SHAPEID;
    }
    public final void setSHAPEID(String sHAPEID) {
        SHAPEID = sHAPEID;
    }
    public final Integer getSTATEFP() {
        return STATEFP;
    }
    public final void setSTATEFP(Integer sTATEFP) {
        STATEFP = sTATEFP;
    }
    public final String getSTATENS() {
        return STATENS;
    }
    public final void setSTATENS(String sTATENS) {
        STATENS = sTATENS;
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
    public final String getSTUSPS() {
        return STUSPS;
    }
    public final void setSTUSPS(String sTUSPS) {
        STUSPS = sTUSPS;
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

}
