package hbase.geometry.simple;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import hbase.CommonSetting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
//import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Util for geometry relation query
 */
public class GeometryRelationship {
    private static String ploygon_sample = "PLOYGON((121.5234 250574, 121.5674 25.0552, 121.5802 25.0261, 121.5099 25.0361, 121.5234 250574))";

    private static Map<String, SimpleFeatureType> featureTypeMap = new HashMap<>();
    private static final GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);
    private static final WKTReader reader = new WKTReader(gf);
    private static final Log LOG = LogFactory.getLog(GeometryRelationship.class);

    /**
     * Tests whether geometry w2 is within geometry w1.
     * @param w1 the geometry in WKT.
     * @param w2 the geometry in WKT.
     */
    public static boolean within(String w1, String w2) {
        try {
            Geometry g1 = reader.read(w1);
            Geometry g2 = reader.read(w2);
            return g2.within(g1);
        } catch (ParseException e) {
            LOG.error("Unexpected throwable object ", e);
            return false;
        }
    }

    /**
     * Parse geometry from WKT(Well Known Text)
     * */
    public static Geometry getGeometry(String WKT) {
        try {
            return reader.read(WKT);
        } catch (ParseException e) {
            LOG.error("Unexpected throwable object ", e);
            return null;
        }
    }

    /**
     * Parse feature from HTable
     * */
    public static SimpleFeature parseHBaseRow(String table, Result value) {
        SimpleFeature f = null;
        try {
            NavigableMap<byte[], byte[]> map = value.getFamilyMap(
                    Bytes.toBytes(CommonSetting.default_column_family));

            if(!featureTypeMap.containsKey(table)) {
                SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();
                b.setName(table);
                b.setCRS(DefaultGeographicCRS.WGS84);
                for(byte[] key:map.keySet()) {
                    String k = new String(key);
                    if(k.equals(CommonSetting.geometry_column))
                        b.add(k, Geometry.class);
                    else
                        b.add(k, String.class);
                }
                featureTypeMap.put(table, b.buildFeatureType());
            }
            SimpleFeatureType featureType = featureTypeMap.get(table);

            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);
            f = builder.buildFeature(new String(value.getRow()));
            for(byte[] key:map.keySet()) {
                String k = new String(key);
                String v = new String(value.getValue(
                        Bytes.toBytes(CommonSetting.default_column_family), Bytes.toBytes(k)));
                if(k.equals(CommonSetting.geometry_column)) {
                    f.setDefaultGeometry(getGeometry(v));
                }else
                    f.setAttribute(k, v);
            }
        } catch (Exception e) {
            LOG.error("Unexpected throwable object ", e);
        }
        return f;
    }
}
