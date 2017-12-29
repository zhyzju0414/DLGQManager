package edu.zju.gis.hbase.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.Geometry.Type;
import com.esri.core.geometry.GeometryEngine;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

public class GeometryRefinementFilter extends FilterBase{

    public boolean filterRow = true;
    public Geometry geo;
    public Type geoType;
    public int geoFieldIndex = -1;


    public GeometryRefinementFilter(byte[] geoArr,byte[] geoType){
        super();
        this.geoType = Utils.getGeometryType(Bytes.toString(geoType));
        this.geo = GeometryEngine.geometryFromWkt(Bytes.toString(geoArr), 0, this.geoType);
    }

    public GeometryRefinementFilter(byte[] geoArr){
        super();
        this.geoType = Utils.getGeometryType("POLYGON");
        this.geo = GeometryEngine.geometryFromWkt(Bytes.toString(geoArr), 0, this.geoType);
    }


    @Override
    public boolean filterAllRemaining() throws IOException {
        // TODO Auto-generated method stub
        return super.filterAllRemaining();
    }

    /**
     * 补充 by zxw 20170601
     * @param cell
     * @return
     * @throws IOException
     */
    @Override
    public ReturnCode filterKeyValue(Cell cell) throws IOException {
        return null;
    }


    @Override
    public void filterRowCells(List<Cell> arg0) throws IOException {
        // TODO Auto-generated method stub
        if(geoFieldIndex==-1){
            for(int i=0;i<arg0.size();i++){
                Cell cell = arg0.get(i);
                if((Bytes.compareTo(cell.getFamily(), Utils.COLUMNFAMILY_GEOMETRY)==0)&&
                        (Bytes.compareTo(arg0.get(i).getQualifier(), Utils.GEOMETRY_QUALIFIER)==0)){
                    geoFieldIndex = i;
                    break;
                }
            }
        }
        String geoStr =Bytes.toString(arg0.get(geoFieldIndex).getValueArray(),
                arg0.get(geoFieldIndex).getValueOffset(), arg0.get(geoFieldIndex).getValueLength());
        Geometry geo1 = GeometryEngine.geometryFromWkt(geoStr, 0, geoType);

        if(!GeometryEngine.intersect(geo1, geo, SpatialReference.create(4490)).isEmpty()){
            filterRow = false;
        }
    }

    @Override
    public boolean filterRow() throws IOException {
        // TODO Auto-generated method stub
        return this.filterRow;
    }

    @Override
    public void reset() throws IOException {
        // TODO Auto-generated method stub
        this.filterRow = true;

    }

    public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
        Preconditions.checkArgument(filterArguments.size() == 2);
        byte [] geoArr = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
        byte [] geoType = ParseFilter.removeQuotesFromByteArray(filterArguments.get(1));

        return new GeometryRefinementFilter(geoArr,geoType);

    }

    public static GeometryRefinementFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        return new GeometryRefinementFilter(pbBytes);
    }

//	  boolean areSerializedFieldsEqual(Filter o) {
//		    if (o == this) return true;
//		    if (!(o instanceof GeometryRefinementFilter)) return false;
//
//		    GeometryRefinementFilter other = (GeometryRefinementFilter)o;
//		    return Bytes.equals(this.getFamily(), other.getFamily())
//		      && Bytes.equals(this.getQualifier(), other.getQualifier())
//		      && this.compareOp.equals(other.compareOp)
//		      && this.getComparator().areSerializedFieldsEqual(other.getComparator())
//		      && this.getFilterIfMissing() == other.getFilterIfMissing()
//		      && this.getLatestVersionOnly() == other.getLatestVersionOnly();
//		  }

//	  FilterProtos.GeometryRefinementFilter convert() {
//		    FilterProtos.SingleColumnValueFilter.Builder builder =
//		      FilterProtos.SingleColumnValueFilter.newBuilder();
//		    if (this.columnFamily != null) {
//		      builder.setColumnFamily(ByteStringer.wrap(this.columnFamily));
//		    }
//		    if (this.columnQualifier != null) {
//		      builder.setColumnQualifier(ByteStringer.wrap(this.columnQualifier));
//		    }
//		    HBaseProtos.CompareType compareOp = CompareType.valueOf(this.compareOp.name());
//		    builder.setCompareOp(compareOp);
//		    builder.setComparator(ProtobufUtil.toComparator(this.comparator));
//		    builder.setFilterIfMissing(this.filterIfMissing);
//		    builder.setLatestVersionOnly(this.latestVersionOnly);
//
//		    return builder.build();
//		  }




}
