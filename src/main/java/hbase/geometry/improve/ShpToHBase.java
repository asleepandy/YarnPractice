package hbase.geometry.improve;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import hbase.CommonSetting;
import org.apache.hadoop.conf.Configuration;
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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Util for shapefile import HBase<br>
 * feature:<br>
 *     1. geometry data place in single column family,
 *     and general attributes in another default column family.<br>
 *
 */
public class ShpToHBase {

    public static byte[] genRowKeyByCentroid(Point centerXY, String type, int timestamp) {
        ByteBuffer bb = ByteBuffer.allocate(24);
        byte[] b = new byte[1];
        new Random().nextBytes(b);
        bb.put(b);
        bb.put(CommonSetting.GEOMETRY_TYPE_KEYMAP.get(type).getBytes());

        double lon = centerXY.getX();
        bb.put((lon >= 0?"E":"W").getBytes());
        bb.putDouble(lon);

        double lat = centerXY.getY();
        bb.put((lat >= 0?"N":"S").getBytes());
        bb.putDouble(lat);

        bb.putInt(timestamp);
        b = bb.array();
        bb.clear();
        return b;
    }

    public static byte[] genRowKeyByEnvelope(Envelope box, String type, int timestamp) {
        ByteBuffer bb = ByteBuffer.allocate(40);
        byte[] b = new byte[1];
        new Random().nextBytes(b);
        bb.put(b);
        bb.put(CommonSetting.GEOMETRY_TYPE_KEYMAP.get(type).getBytes());
        bb.putInt(timestamp);
        bb.putDouble(box.getMinX());
        bb.putDouble(box.getMinY());
        bb.putDouble(box.getMaxX());
        bb.putDouble(box.getMaxY());
        bb.put("00".getBytes());

        b = bb.array();
        bb.clear();
        return b;
    }

    public static void main(final String[] args) throws Exception {
        Configuration conf = CommonSetting.getHbaseConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage: shp2hbase <in> ");
            System.exit(2);
        }

        // resolve shp
        System.out.println("Resolving...");
        String filepath = otherArgs[0];
        File file = new File(filepath);
        file.setReadOnly();

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
        Connection conn = ConnectionFactory.createConnection(conf);
        System.out.println("HBase connected.");

        // set table name same with file name
        String _tableName = file.getName().split("\\.")[0];
        if(_tableName.isEmpty()) _tableName = String.format("%s_%d", typeName, file.length());

        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf(_tableName);
        if(!admin.tableExists(tn)) {
            HTableDescriptor desc = new HTableDescriptor(tn);
            desc.addFamily(new HColumnDescriptor(Bytes.toBytes(CommonSetting.default_column_family)));
            desc.addFamily(new HColumnDescriptor(Bytes.toBytes(CommonSetting.geometry_column_family)));
            admin.createTable(desc);
            System.out.println(String.format("HTable %s created.", _tableName));
        }
        Table table = conn.getTable(tn);
        List<Row> batch = new ArrayList<>();
        Object[] result = null;

        // import feature into hbase
        int total = collection.size();
        int flag = 0, percent = 0;
        System.out.println(String.format("Prepare import %d features.", total));
        long time = System.currentTimeMillis();
        try (FeatureIterator<SimpleFeature> features = collection.features()) {
            while (features.hasNext()) {
                SimpleFeature feature = features.next();
                Geometry geom = (Geometry) feature.getDefaultGeometryProperty().getValue();
                String type = geom.getGeometryType();
                byte[] rowKey = "Point".equals(type) ?
                        genRowKeyByCentroid(geom.getCentroid(), type, (int)(System.currentTimeMillis()/1000L)):
                        genRowKeyByEnvelope(geom.getEnvelopeInternal(), type, (int)(System.currentTimeMillis()/1000L));

                Put put = new Put(rowKey);
                put.addColumn(Bytes.toBytes(CommonSetting.geometry_column_family),
                        Bytes.toBytes(CommonSetting.geometry_column),
                        Bytes.toBytes(geom.toString())
                );
                for (Property attribute : feature.getProperties()) {
                    if("the_geom".equals(attribute.getName().toString())) continue;

                    put.addColumn(Bytes.toBytes(CommonSetting.default_column_family),
                            Bytes.toBytes(attribute.getName().toString()),
                            Bytes.toBytes(attribute.getValue().toString())
                    );
                }
                batch.add(put);
//                puts.add(put);
                if(batch.size() == 10000) {
                    flag += batch.size();
//                    table.put(puts);
//                    puts.clear();
                    try {
                        result = new Object[batch.size()];
                        table.batch(batch, result);
                        int p = flag*100/total;
                        if(p > 0 && p != percent) {
                            percent = p;
                            System.out.println(String.format("%d%%", percent));
                        }
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
            features.close();
            time = System.currentTimeMillis() - time;
        }

        System.out.println("100%");
        System.out.println(String.format("Import completed (%d ms).", time));
        batch.clear();
        table.close();
        conn.close();
        dataStore.dispose();
    }
}
