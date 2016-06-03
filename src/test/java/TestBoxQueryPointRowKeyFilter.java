import com.vividsolutions.jts.geom.*;
import hbase.filter.BoxQueryPointRowKeyFilter;
import hbase.geometry.improve.ShpToHBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by andy.lai on 2016/6/2.
 */
public class TestBoxQueryPointRowKeyFilter {

    private String selectedArea = "POLYGON((121.5234 25.0574, 121.5674 25.0552, 121.5802 25.0261, 121.5099 25.0361, 121.5234 25.0574))";

    @Test
    public void testEnvelopeRowKey() {
        try {
            GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);
//            LineString l = gf.createLineString(new Coordinate[]{
//                    new Coordinate(121.5235, 25.0531), new Coordinate(121.5399, 25.0455), new Coordinate(121.5629,25.0311)});
            Point p = gf.createPoint(new Coordinate(121.5235, 25.0531));
            byte[] b = ShpToHBase.genRowKeyByEnvelope(
                    p.getEnvelopeInternal(), p.getGeometryType(), (int)(System.currentTimeMillis()/1000L));
            String s = new String(b);
            System.out.println(s);
            System.out.println(s.length());
            BoxQueryPointRowKeyFilter f = new BoxQueryPointRowKeyFilter(this.selectedArea);
            Assert.assertFalse(f.filterRowKey(b, 0, b.length));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
