package edu.zju.gis.hbase.tool;

//cc CustomFilter Implements a filter that lets certain rows pass
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.Geometry.Type;
import com.esri.core.geometry.GeometryEngine;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Implements a custom filter for HBase. It takes a value and compares
 * it with every value in each KeyValue checked. Once there is a match
 * the entire row is passed, otherwise filtered out.
 */
//vv CustomFilter
public class PointContainFilter extends FilterBase {

    private double x;
    private double y;

    private boolean filterRow = true;
    private Point point;

    private boolean foundwkt = false;
    private boolean foundtype = false;
    private String geoType = null;
    private String wkt = null;



    public PointContainFilter() {
        super();
    }

    public PointContainFilter(double x,double y) {
        this.x = x;
        this.y = y;
        point = new Point(x,y);
    }

    @Override
    public void reset() {
        this.filterRow = true; // co CustomFilter-2-Reset Reset filter flag for each new row being tested.
        this.foundwkt = false;
        this.foundtype = false;
        this.geoType = null;
        this.wkt = null;
    }


    /**
     * 修改 by zxw 20170601
     * @param cell
     * @return
     * @throws IOException
     */
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
        if(!(foundwkt&&foundtype)){
            KeyValue keyValue = KeyValueUtil.ensureKeyValue(cell);
            if(CellUtil.matchingColumn(cell,Bytes.toBytes("CFGeometry"), Bytes.toBytes("Geometry"))){
                foundwkt = true;
                wkt = Bytes.toString(keyValue.getValueArray(),
                        keyValue.getValueOffset(), keyValue.getValueLength());
            }

            if(CellUtil.matchingColumn(cell,Bytes.toBytes("CFProperty"), Bytes.toBytes("FeatureType"))){
                geoType = Bytes.toString(keyValue.getValueArray(),
                        keyValue.getValueOffset(), keyValue.getValueLength());
                foundtype = true;
            }
        }else{
            return ReturnCode.INCLUDE;
        }

        if(foundwkt&&foundtype){
            Type t = getGeometryType(geoType);
            Geometry geo1=null;
            try{
                geo1 = GeometryEngine.geometryFromWkt(wkt, 0, t);
            }catch(IllegalArgumentException e){
                return ReturnCode.NEXT_ROW;
            }
            if(geo1!=null){
                if(GeometryEngine.contains(geo1, this.point, SpatialReference.create(4490))){
                    filterRow = false;
                }else
                    return ReturnCode.NEXT_ROW;
            }
        }

        return ReturnCode.INCLUDE;
    }



    @Override
    public boolean filterRow() {
        return filterRow; // co CustomFilter-5-FilterRow Here the actual decision is taking place, based on the flag status.
    }

    @Override
    public byte [] toByteArray() {
        FilterProtos.PointContainFilter.Builder builder =
                FilterProtos.PointContainFilter.newBuilder();
        builder.setX(x); // co CustomFilter-6-Write Writes the given value out so it can be send to the servers.
        builder.setY(y);
        return builder.build().toByteArray();
    }

    public static Type getGeometryType(String geoType){
        if(geoType.toLowerCase().equals("point")){
            return Type.Point;
        }
        if(geoType.toLowerCase().equals("polygon")){
            return Type.Polygon;
        }
        if(geoType.toLowerCase().equals("polyline")){
            return Type.Polyline;
        }
        if(geoType.toLowerCase().equals("multipolygon")){
            return Type.Polygon;
        }
        return null;
    }

    //@Override
    public static Filter parseFrom(final byte[] pbBytes)
            throws DeserializationException {
        FilterProtos.PointContainFilter proto;
        try {
            proto = FilterProtos.PointContainFilter.parseFrom(pbBytes); // co CustomFilter-7-Read Used by the servers to establish the filter instance with the correct values.
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new PointContainFilter(proto.getX(),proto.getY());
    }

}