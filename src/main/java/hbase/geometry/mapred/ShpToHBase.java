package hbase.geometry.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.GenericOptionsParser;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Util for shapefile import HBase
 */
public class ShpToHBase {

    /**
     * default column family
     * */
    private final static String _column_family = "d";

    public void connectDb() {

    }

    public static void main(final String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create(new Configuration());
        conf.set("hbase.zookeeper.quorum", "vm1-64");
        conf.setInt("hbase.zookeeper.property.clientPort", 2181);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage: shp2hbase <in> ");
            System.exit(2);
        }

        // resolve shp
        System.out.println("Resolving...");
        String filepath = otherArgs[0];
//        String filepath = "/Users/andy.lai/Documents/vagrant_img/virtual-hadoop-cluster/shp/points.shp";
//        Path p = new Path(filepath);
//        FileSystem fs = p.getFileSystem(new Configuration());
        File file = new File(filepath);

        Map<String, Object> map = new HashMap<>();
        map.put(ShapefileDataStoreFactory.URLP.key, file.toURI().toURL());
        map.put(ShapefileDataStoreFactory.DBFCHARSET.key, Charset.forName("utf-8"));

        DataStore dataStore = DataStoreFinder.getDataStore(map);
        String typeName = dataStore.getTypeNames()[0];
        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore
                .getFeatureSource(typeName);
        Filter filter = Filter.INCLUDE; // ECQL.toFilter("BBOX(THE_GEOM, 10,20,30,40)")
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);

        // hbase connection init
        String _tableName = file.getName().split("\\.")[0];
        if(_tableName.isEmpty()) _tableName = String.format("%s_%d", typeName, file.length());
        Connection conn = ConnectionFactory.createConnection(conf);
        System.out.println("HBase connected.");

        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(_tableName);
        if(!admin.tableExists(tn)) {
            HTableDescriptor desc = new HTableDescriptor(tn);
            HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes(_column_family));
            desc.addFamily(coldef);
            admin.createTable(desc);
            System.out.println(String.format("HTable %s created.", _tableName));
        }
        Table table = conn.getTable(tn);
        List<Row> batch = new ArrayList<>();
        Object[] result = null;
//        List<Put> puts = new ArrayList<>();

        // import feature into hbase
        int total = collection.size();
        int flag = 0;
        System.out.println(String.format("Prepare import %d features.", total));
        long time = System.currentTimeMillis();
        try (FeatureIterator<SimpleFeature> features = collection.features()) {
            while (features.hasNext()) {
                SimpleFeature feature = features.next();
                Put put = new Put(Bytes.toBytes(
                        String.format("%s", feature.getID()))
                );
                put.addColumn(Bytes.toBytes(_column_family), Bytes.toBytes("the_geom"),
                        Bytes.toBytes(feature.getDefaultGeometryProperty().getValue().toString())
                );
                for (Property attribute : feature.getProperties()) {
                    put.addColumn(Bytes.toBytes(_column_family),
                            Bytes.toBytes(attribute.getName().toString()),
                            Bytes.toBytes(attribute.getValue().toString())
                    );
                }
                batch.add(put);
//                puts.add(put);
                if(flag <= 8 && total/batch.size() == 10) {
                    flag++;
//                    table.put(puts);
//                    puts.clear();
                    try {
                        result = new Object[batch.size()];
                        table.batch(batch, result);
                        System.out.println(String.format("%d%%",flag*10));
                    } catch (Exception e) {
                        System.out.println(String.format("Batch error: %s", e.toString()));
                    } finally {
                        batch.clear();
                    }
                }
            }
            if(!batch.isEmpty()) {
                result = new Object[batch.size()];
                table.batch(batch, result);
            }
            time = System.currentTimeMillis() - time;
        }

        System.out.println("100%");
        System.out.println(String.format("Import completed (%d ms).", time));
        batch.clear();
        table.close();
        conn.close();
    }
}
