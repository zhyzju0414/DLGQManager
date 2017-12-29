package edu.zju.gis.hbase.tool;

//cc CustomFilter Implements a filter that lets certain rows pass
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.esri.core.geometry.Geometry;
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
public class CustomFilter extends FilterBase {

    private byte[] value = null;
    private boolean filterRow = true;
    private Geometry geo;

    private boolean foundwkt = false;
    private boolean foundtype = false;
    private String geoType = null;
    private String wkt = null;



    public CustomFilter() {
        super();
    }

    public CustomFilter(byte[] value) {
        this.value = value;
        geo = GeometryEngine.geometryFromWkt(Bytes.toString(value), 0, Type.Polygon);
    }

    @Override
    public void reset() {
        this.filterRow = true;
        this.foundwkt = false;
        this.foundtype = false;
        this.geoType =  null;
        this.wkt = null;
    }


    /**
     * 修改 by zxw 20170601
     * @param cell
     * @return
     * @throws IOException
     */
    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
        System.out.println("=================enter filterKeyValue===================="+"foundwkt="+String.valueOf(foundwkt)+"foundType="+String.valueOf(foundtype));

        if(!(foundwkt&&foundtype)){
            System.out.println("进入 false foundwkt foundtype");
            KeyValue keyValue = KeyValueUtil.ensureKeyValue(cell);

        if(CellUtil.matchingColumn(cell,Bytes.toBytes("CFGeometry"), Bytes.toBytes("Geometry"))) {
//            if(keyValue.matchingColumn(Bytes.toBytes("CFGeometry"), Bytes.toBytes("Geometry"))){
                foundwkt = true;
                wkt = Bytes.toString(keyValue.getValueArray(),
                        keyValue.getValueOffset(), keyValue.getValueLength());
            }
            if(CellUtil.matchingColumn(cell,Bytes.toBytes("CFProperty"), Bytes.toBytes("FeatureType"))) {
           // if(keyValue.matchingColumn(Bytes.toBytes("CFProperty"), Bytes.toBytes("FeatureType"))){
                geoType = Bytes.toString(keyValue.getValueArray(),
                        keyValue.getValueOffset(), keyValue.getValueLength());
                foundtype = true;
            }
        }else{
            return ReturnCode.INCLUDE;
        }

        if(foundwkt&&foundtype){
            System.out.println("进入 true foundwkt foundtype");
            Type t = getGeometryType(geoType);
            Geometry geo1=null;
            try{
                System.out.println("WKT = "+wkt);
                geo1 = GeometryEngine.geometryFromWkt(wkt, 0, t);
//                System.out.println(GeometryEngine.geometryToWkt(geo1,0));
            }catch(IllegalArgumentException e){
                return ReturnCode.NEXT_ROW;
            }
            if(geo1!=null) {
                System.out.println("geo1 !=null");
                if (!GeometryEngine.intersect(this.geo, geo1, SpatialReference.create(4490)).isEmpty()) {
                    System.out.println("youjiaji: " + GeometryEngine.geometryToWkt(geo1, 0));
                    filterRow = false;
                } else
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
       //] System.out.println("=================enter toByteArray====================");
        FilterProtos.CustomFilter.Builder builder =
                FilterProtos.CustomFilter.newBuilder();
        if (value != null) builder.setValue(ByteString.copyFrom(ByteBuffer.wrap(value))); // co CustomFilter-6-Write Writes the given value out so it can be send to the servers.
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
        FilterProtos.CustomFilter proto;
        try {
            proto = FilterProtos.CustomFilter.parseFrom(pbBytes); // co CustomFilter-7-Read Used by the servers to establish the filter instance with the correct values.
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new CustomFilter(proto.getValue().toByteArray());
    }

}