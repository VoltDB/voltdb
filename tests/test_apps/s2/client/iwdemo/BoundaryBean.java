package iwdemo;

/**
 * Boundaries are the same for states and counties.
 * @author bwhite
 *
 */
public class BoundaryBean {
    private String SHAPEID;
    private String SHAPENUMBER;
    private String SHAPECOMPONENT;
    private Double X;
    private Double Y;
    public BoundaryBean() {

    }
    public final String getSHAPEID() {
        return SHAPEID;
    }
    public final void setSHAPEID(String SHAPEID) {
        this.SHAPEID = SHAPEID;
        SHAPENUMBER = GeoUtils.getShapeNumber(SHAPEID);
        SHAPECOMPONENT = GeoUtils.getShapeComponent(SHAPEID);
    }
    public final String getSHAPENUMBER() {
        return SHAPENUMBER;
    }
    public final String getSHAPECOMPONENT() {
        return SHAPECOMPONENT;
    }
    public final Double getX() {
        return X;
    }
    public final void setX(Double x) {
        X = x;
    }
    public final Double getY() {
        return Y;
    }
    public final void setY(Double y) {
        Y = y;
    }
}
