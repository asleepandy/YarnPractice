import com.vividsolutions.jts.geom.*;
import hbase.geometry.improve.ShpToHBase;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by andy.lai on 2016/5/8.
 */
public class lab {

    public static void main(String[] args) {
        double lon = -122.345678;
        double lat = 25.678901;

//        byte[] buf = ByteBuffer.allocate(8).putDouble(lon).array();
//        System.out.println(new String(buf));
//        System.out.println(Arrays.toString(buf));

        GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);
        Point p = gf.createPoint(new Coordinate(lon, lat));
        System.out.println(Float.valueOf("122.345678"));
        System.out.println(p.getY());
        int t = (int)(System.currentTimeMillis()/1000L);
        System.out.println(t);
        byte[] b = ShpToHBase.genRowKeyByCentroid(p, p.getGeometryType(), t);
        System.out.println(b.length);
        System.out.println(new String(b));
        System.out.println(Arrays.toString(b));

//        ByteBuffer bb = ByteBuffer.wrap(b);
////        bb.position(3);
//        System.out.println(bb.getFloat(3));
////        bb.position(8);
//        System.out.printf("%f\n", bb.getFloat(8));
//        System.out.printf("%d\n", bb.getInt(12));

        LineString l = gf.createLineString(new Coordinate[]{new Coordinate(1,1), new Coordinate(2,2), new Coordinate(3,1)});
        b = ShpToHBase.genRowKeyByEnvelope(l.getEnvelopeInternal(), l.getGeometryType(), t);
        System.out.println(b.length);
        System.out.println(new String(b));
        System.out.println(Arrays.toString(b));
//        Envelope env = p.getEnvelopeInternal();
//        System.out.println(String.format("%s, %s", env.getMinX(), env.getMinY()));
//        System.out.println(String.format("%s, %s", env.getMaxX(), env.getMaxY()));

        System.out.println(new String(new byte[] {"1".getBytes()[0]}));
    }
}
