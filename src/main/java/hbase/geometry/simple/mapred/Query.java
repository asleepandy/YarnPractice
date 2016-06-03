package hbase.geometry.simple.mapred;

import hbase.CommonSetting;
import hbase.geometry.simple.GeometryRelationship;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.NavigableMap;

/**
 * MapReduce job for spatial query
 */
public class Query extends Configured implements Tool {
    private static String params_geom_key = "geometry_src";

    public static class MyMapper extends TableMapper<Text, MapWritable> {

        private Text text = new Text();

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String params_geom_in_wkt = conf.get(params_geom_key);
            String the_geom_in_wkt = new String(value.getValue(Bytes.toBytes("d"), Bytes.toBytes("the_geom")));
            if(GeometryRelationship.within(params_geom_in_wkt, the_geom_in_wkt)) {
//                SimpleFeature f = GeometryRelationship.parseHBaseRow(conf.get(TableInputFormat.INPUT_TABLE), value);
                NavigableMap<byte[], byte[]> map = value.getFamilyMap(
                        Bytes.toBytes(CommonSetting.default_column_family));
                MapWritable mw = new MapWritable();
                for(byte[] key:map.keySet()) {
                    Text k = new Text(key);
                    Text v = new Text(value.getValue(
                            Bytes.toBytes(CommonSetting.default_column_family), Bytes.toBytes(k.toString())));
                    mw.put(k, v);
                }

                text.set(row.get());
                context.write(text, mw);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, MapWritable, Text, Text> {

        private Text oValue = new Text();

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
            for (MapWritable val : values) {
//                SimpleFeature f = (SimpleFeature) val.get();
                oValue = (Text) val.get(new Text("name"));
                if(oValue == null || "".equals(oValue.toString()))
                    oValue = (Text) val.get(new Text("type"));
                key.set(String.format("%s:%s", key.toString(), oValue.toString()));
                oValue = (Text) val.get(new Text(CommonSetting.geometry_column));
                context.write(key, oValue);
            }

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = CommonSetting.getHbaseConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: query <wkt_of_geometry> <table_in> <out>");
            System.exit(2);
        }
//        final String polygon_sample = "PLOYGON((121.5234 250574, 121.5674 25.0552, 121.5802 25.0261, 121.5099 25.0361, 121.5234 250574))";

        conf.set(params_geom_key, otherArgs[0]);
        Job job = Job.getInstance(conf, "SpatialQuery_Within");
        job.setJarByClass(Query.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(
                otherArgs[1],           // input table
                scan,               // Scan instance to control CF and attribute selection
                MyMapper.class,     // mapper class
                Text.class,         // mapper output key
                MapWritable.class,  // mapper output value
                job);
        job.setReducerClass(MyReducer.class);    // reducer class
        job.setNumReduceTasks(1);    // at least one, adjust as required
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        return job.waitForCompletion(true) ? 0 : 1;

        /*
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("vagrant");
        int result = ugi.doAs(new PrivilegedExceptionAction<Integer>() {
            @Override
            public Integer run() throws Exception {
            }
        });*/
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "vagrant");
        int result = ToolRunner.run(new Query(), args);
        System.exit(result);
    }
}
