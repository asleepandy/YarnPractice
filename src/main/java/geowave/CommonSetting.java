package geowave;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CommonSetting {
    private static Properties Config = new Properties();

    private static void loadProperties() {
        InputStream input = null;
        try {
            input = CommonSetting.class.getClassLoader().getResourceAsStream("geowave.properties");
            Config.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(input != null)
                try {
                    input.close();
                }catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    public static Properties getConfig() {
        if(Config.isEmpty()) loadProperties();
        return Config;
    }

    public static BasicAccumuloOperations getAccumuloOperationsInstance()
            throws AccumuloException, AccumuloSecurityException {

        getConfig();

        return new BasicAccumuloOperations(
                Config.getProperty("zookeepers"),
                Config.getProperty("instance"),
                Config.getProperty("user"),
                Config.getProperty("password"),
                Config.getProperty("namespace"));
    }

    public static DataStore getDataStore()
            throws AccumuloException, AccumuloSecurityException {

        final BasicAccumuloOperations instance = getAccumuloOperationsInstance();
        return new AccumuloDataStore(
                new AccumuloIndexStore(instance),
                new AccumuloAdapterStore(instance),
                new AccumuloDataStatisticsStore(instance),
                new AccumuloSecondaryIndexDataStore(instance),
                new AccumuloAdapterIndexMappingStore(instance),
                instance);
    }
}
