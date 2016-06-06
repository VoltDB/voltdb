package genqa.procedures;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;

public class SampleGeoRecord extends SampleRecord {

    public final Object type_null_geography;
    public final Object type_not_null_geography;
    public final Object type_null_geography_point;
    public final Object type_not_null_geography_point;
    
    public SampleGeoRecord(long rowid, Random rand) {
       super(rowid,rand);
       this.type_null_geography      = nextGeography(rand, true, 128, 1024);
       this.type_not_null_geography  = nextGeography(rand, 128, 1024);
       this.type_null_geography_point = nextGeographyPoint(rand,true,128,1024);
       this.type_not_null_geography_point = nextGeographyPoint(rand,128,1024);
        
    }

    private static Object nextGeography(Random rand, int minLength, int maxLength)
    {
        return nextGeography(rand, false, minLength, maxLength);
    }

    // POLYGON ( ( 1.5 3.0, 0.0 0.0, 3.0 0.0, 1.5 3.0 ) )
    private static Object nextGeography(Random rand, boolean isNullable, int minLength, int maxLength)
    {
        if (isNullable && rand.nextBoolean()) return null;
        if (isNullable && rand.nextBoolean()) return null;
        float pointPart = 91;
        while ( pointPart > 90.0 || pointPart < -90.0 ) {
            pointPart = rand.nextFloat()*rand.nextLong();
        } 
        // this needs to be a valid polygon where segments don't cross over each other and the
        // start and end point are equal.
        String wktPoly = "POLYGON(( 0.0 0.0,"+String.valueOf(pointPart)+ " "+String.valueOf(pointPart)+", 0.0 0.000001,0.0 0.0))";
        return GeographyValue.fromWKT(wktPoly);
    }

    private static Object nextGeographyPoint(Random rand, int minLength, int maxLength)
    {
        return nextGeographyPoint(rand, false, minLength, maxLength);
    }

    // Geography points have a syntax like this:
    // POINT(-74.0059 40.7127)
    private static Object nextGeographyPoint(Random rand, boolean isNullable, int minLength, int maxLength)
    {
        if (isNullable && rand.nextBoolean()) return null;
        float pointPart = 91;
        while ( pointPart > 90.0 || pointPart < -90.0 ) {
            pointPart = rand.nextFloat()*rand.nextLong();
        } 
        String wktPoint = "POINT("+String.valueOf(pointPart)+ " "+String.valueOf(pointPart)+")";
        return GeographyPointValue.fromWKT(wktPoint);
    }
}    