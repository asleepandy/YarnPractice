package hbase.geometry.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Common setting handler
 */
public class CommonSetting {
    /**
     * default column family
     * */
    public final static String default_column_family = "d";
    public final static String geometry_column = "the_geom";

    public static Configuration getHbaseConf() {
        Configuration conf = HBaseConfiguration.create(new Configuration());
        conf.set("hbase.zookeeper.quorum", "vm1-64");
        conf.setInt("hbase.zookeeper.property.clientPort", 2181);
        return conf;
    }
}
