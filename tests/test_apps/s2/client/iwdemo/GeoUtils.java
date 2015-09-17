package iwdemo;

public class GeoUtils {
    public static String getShapeNumber(String shapeId) {
        int idx = shapeId.indexOf(".");
        if (idx < 0) {
            return shapeId;
        } else {
            return shapeId.substring(0, idx);
        }
    }
    public static String getShapeComponent(String shapeId) {
        int idx = shapeId.indexOf(".");
        if (idx < 0) {
            return "0";
        } else {
            return shapeId.substring(idx+1);
        }
    }

}
