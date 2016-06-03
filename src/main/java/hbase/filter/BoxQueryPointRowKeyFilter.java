package hbase.filter;

import com.google.protobuf.InvalidProtocolBufferException;
import com.vividsolutions.jts.geom.*;
import hbase.CommonSetting;
import hbase.geometry.simple.GeometryRelationship;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.geotools.geometry.jts.JTS;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Custom filter for spatial row key scan.
 * Created by asleepandy on 2016/5/14.
 */
public class BoxQueryPointRowKeyFilter extends FilterBase {

    private String value = null;
    private boolean filterRow = false;
    private GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);
    private boolean test = true;
    private static final Log LOG = LogFactory.getLog(BoxQueryPointRowKeyFilter.class);

    public BoxQueryPointRowKeyFilter(String value) {
        this.value = value;
    }

    /**
     * RowKey byte array format:
     *  0    1     2  3 4 5 6 7 8 9 10  11 12 13 14 15 16 17 18 19 20 21 22 23
     *  |    |     |  |--------------|  |  |---------------------| |---------|
     * flag class E/W       lon        N/S           lat            timestamp
     *
     * E-East, W-West, N-North, S-South
     */
    private Geometry parsePointFromRowKey(byte[] buffer, int offset) {
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        double lon = bb.getDouble(offset+3);
        double lat = bb.getDouble(offset+12);
        return gf.createPoint(new Coordinate(lon, lat));
    }

    /**
     * RowKey byte array format:
     *  0    1    2  3  4  5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39
     *  |    |    |--------| |-----------------| |---------------------| |---------------------| |---------------------|  |  |
     * flag class timestamp       x1                 y1                      x2                      y2                   0  0
     *
     * E-East, W-West, N-North, S-South
     */
    private Geometry parseEnvelopeFromRowKey(byte[] buffer, int offset) {
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        String type = new String(new byte[]{bb.get(offset+1)});
        double x1 = bb.getDouble(offset+6);
        double y1 = bb.getDouble(offset+14);
        double x2 = bb.getDouble(offset+22);
        double y2 = bb.getDouble(offset+30);
//        if(test) {
//            test = false;
//            LOG.info(String.format("x1:%d, y1:%d, x2:%d, y2:%d", x1, y1, x2, y2));
//        }
//        if(x1 == x2 && y1 == y2)
        if(CommonSetting.GEOMETRY_TYPE_KEYMAP.get("Point").equals(type))
            return gf.createPoint(new Coordinate(x1, y1));
        else
            return JTS.toGeometry(new Envelope(x1, x2, y1, y2), gf);
    }

    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
        try {
            Geometry p = (length <= 24 ?
                    parsePointFromRowKey(buffer, offset):
                    parseEnvelopeFromRowKey(buffer, offset));

            this.filterRow = !GeometryRelationship.getGeometry(this.value).intersects(p);
        } catch(Exception e) {
            LOG.error("Unexpected throwable object ", e);
        }
        return this.filterRow;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        return ReturnCode.INCLUDE;
    }

    @Override
    public void reset() throws IOException {
        this.filterRow = true;
    }

    @Override
    public boolean filterRow() throws IOException {
        return this.filterRow;
    }

    @Override
    public byte[] toByteArray() {
        FilterProtos.Filter.Builder builder = FilterProtos.Filter.newBuilder();
        if(this.value != null) builder.setName(this.value);
        return builder.build().toByteArray();
    }

    public static Filter parseFrom(final byte [] pbBytes) throws DeserializationException{
        FilterProtos.Filter proto;
        try {
            proto = FilterProtos.Filter.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new BoxQueryPointRowKeyFilter(proto.getName());
    }
}
