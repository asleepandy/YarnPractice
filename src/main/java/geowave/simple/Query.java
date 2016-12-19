package geowave.simple;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import geowave.CommonSetting;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import org.apache.log4j.Logger;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;

public class Query {
    private static Logger log = Logger.getLogger(Query.class);
    private static DataStore geowaveDataStore;
    private static AdapterStore adapterStore;

    /**
     * arguments:<accumulo host:port> <accumulo instance name> <user> <password> <table namespace>
     * example:'vm1-64:2181', 'accumulo', 'root', '1523', 'geowave.test'
     * @param args
     */
    public static void main(String[] args) {
        try {
            BasicAccumuloOperations bao = CommonSetting.getAccumuloOperationsInstance();
            geowaveDataStore = new AccumuloDataStore(bao);
            adapterStore = new AccumuloAdapterStore(bao);

            Query query = new Query();
            query.pointQueryCase();
        }
        catch (final Exception e) {
            log.error("Error creating BasicAccumuloOperations", e);
            System.exit(1);
        }
    }

    private void pointQueryCase() throws ParseException, IOException {
        log.info("Running Point Query Case 1");
        // First, we need to obtain the adapter for the SimpleFeature we want to query.
        // We'll query basic-feature in this example.
        // Obtain adapter for our "basic-feature" type
        ByteArrayId bfAdId = new ByteArrayId("basic-feature");
        FeatureDataAdapter bfAdapter = (FeatureDataAdapter) adapterStore.getAdapter(bfAdId);

        // Define the geometry to query. We'll find all points that fall inside that geometry
        String queryPolygonDefinition = "POLYGON (( "
                + "-90 -45, "
                + "-90 45, "
                + "90 45, "
                + "90 -45, "
                + "-90 -45" + "))";
        Geometry queryPolygon = new WKTReader(
                JTSFactoryFinder.getGeometryFactory()).read(queryPolygonDefinition);

        // Perform the query.Parameters are
        /**
         * 1- Adapter previously obtained from the feature name. 2- Default
         * spatial index. 3- A SpatialQuery, which takes the query geometry -
         * aka Bounding box 4- Filters. For this example, no filter is used. 5-
         * Limit. Same as standard SQL limit. 0 is no limits. 6- Accumulo
         * authorizations. For our mock instances, "root" works. In a real
         * Accumulo setting, whatever authorization is associated to the user in
         * question.
         */

        final QueryOptions options = new QueryOptions(
                bfAdapter,
                new SpatialIndexBuilder().createIndex());
//        options.setAuthorizations(new String[] {
//                "root"
//        });
        int count = 0;
        try (final CloseableIterator<SimpleFeature> iterator = geowaveDataStore.query(
                options,
                new SpatialQuery(
                        queryPolygon))) {

            while (iterator.hasNext()) {
                SimpleFeature sf = iterator.next();
                log.info("Obtained SimpleFeature " + sf.getName().toString() + " - " + sf.getAttribute("Latitude"));
                count++;
                System.out.println("Query match: " + sf.getID());
            }
            log.info(String.format("Should have obtained %d features.", count));
        }
    }


}
