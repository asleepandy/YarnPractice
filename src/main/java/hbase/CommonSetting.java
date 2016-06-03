package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Common setting handler
 */
public class CommonSetting {
    /**
     * default column family
     * */
    public final static String default_column_family = "d";
    public final static String geometry_column_family = "g";
    public final static String geometry_column = "the_geom";
    public static Map<String, String> GEOMETRY_TYPE_KEYMAP = new HashMap<>();

    static {
        GEOMETRY_TYPE_KEYMAP.put("Point", "1");
        GEOMETRY_TYPE_KEYMAP.put("LineString", "2");
        GEOMETRY_TYPE_KEYMAP.put("Polygon", "3");
        GEOMETRY_TYPE_KEYMAP.put("MultiPoint", "4");
        GEOMETRY_TYPE_KEYMAP.put("MultiLineString", "5");
        GEOMETRY_TYPE_KEYMAP.put("MultiPolygon", "6");
        GEOMETRY_TYPE_KEYMAP.put("LineRing", "7");
    }

    public static Configuration getHbaseConf() {
        Configuration conf = HBaseConfiguration.create(new Configuration());
        conf.set("hbase.zookeeper.quorum", "vm1-64");
        conf.setInt("hbase.zookeeper.property.clientPort", 2181);

        //for execute remote job from IDEA/eclipse
        conf.set("mapred.jar",
                "/Users/andy.lai/Documents/src/git/YarnPractice/build/classes/artifacts/spatial_hbase/spatial_hbase.jar");

        return conf;
    }

    public static void main(String[] args) {
        String s = "12";
        System.out.println(s.getBytes());
    }
}
