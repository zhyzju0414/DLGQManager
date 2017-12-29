package edu.zju.gis.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Geometry.Type;
import com.esri.core.geometry.GeometryEngine;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticRequest;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticRequest.GetInfo;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticResponse;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticResponse.Builder;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticResponse.SpatialStatisticInfo;


public class SpatialStatisticEndPoint extends SpatialStatisticProtos.SpatialStatisticService
        implements Coprocessor, CoprocessorService{

    public SpatialStatisticEndPoint() {

    }

    private RegionCoprocessorEnvironment env;

    @Override
    public void getStatisticInfo(RpcController controller,
                                 SpatialStatisticRequest request,
                                 RpcCallback<SpatialStatisticResponse> done) {
        // TODO Auto-generated method stub
        Map<String,edu.zju.gis.hbase.entity.StatisticInfo> statisticResult = new HashMap<String,edu.zju.gis.hbase.entity.StatisticInfo>();

        String wkt = request.getGeowkt();
        Geometry geo =  GeometryEngine.geometryFromWkt(wkt, 0, Type.Polygon);
        List<GetInfo> getInfo = request.getGetListList();
        Region currentRegion = env.getRegion();

        Filter colimnFilter = GetwktColumnFilter();
        for(int i=0;i<getInfo.size();i++){
            String rowKey = getInfo.get(i).getRowkey();

            String cc = GetCategoryCode(rowKey);
            if(!statisticResult.containsKey(cc)){
                statisticResult.put(cc, new edu.zju.gis.hbase.entity.StatisticInfo());
            }

            boolean clipRequired = getInfo.get(i).getCliprequired();
            Get get = new Get(Bytes.toBytes(rowKey));
            get.setFilter(colimnFilter);
            try {
                if(!currentRegion.getRegionInfo().containsRow(get.getRow()))
                    continue;
                Result result = currentRegion.get(get);
                if(result!=null&&!result.isEmpty()){
                    double area = 0.0;
                    double length = 0.0;
                    String geowkt = null;
                    for(Cell cell:result.rawCells()){
                        byte[] qualifier = cell.getQualifier();
                        if(Bytes.equals(qualifier,Bytes.toBytes("AREA"))){
                            area=Double.parseDouble(Bytes.toString(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength()));
                        }else if(Bytes.equals(qualifier,Bytes.toBytes("LENGTH"))){
                            length=Double.parseDouble(Bytes.toString(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength()));
                        }else if(Bytes.equals(qualifier,Bytes.toBytes("Geometry"))){
                            geowkt = Bytes.toString(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength());
                        }
                    }
                    if(clipRequired){
                        Geometry geo1 =   GeometryEngine.geometryFromWkt(geowkt, 0, Type.Polygon);
                        Geometry intersectGeo = GeometryEngine.intersect(geo, geo1, null);
                        if(!intersectGeo.isEmpty()){
                            area = intersectGeo.calculateArea2D();
                            length = intersectGeo.calculateLength2D();
                            statisticResult.get(cc).COUNT++;
                        }
                    }else{
                        statisticResult.get(cc).COUNT++;
                    }
                    statisticResult.get(cc).AREA+=area;
                    statisticResult.get(cc).LENGTH+=length;

                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        Builder builder = SpatialStatisticResponse.newBuilder();
        for(Map.Entry<String,edu.zju.gis.hbase.entity.StatisticInfo> entry:statisticResult.entrySet()){
            edu.zju.gis.hbase.entity.StatisticInfo statisticinfo = entry.getValue();
            builder.addSpatialStatisticInfo(SpatialStatisticInfo.newBuilder().setArea(statisticinfo.AREA).setCount(statisticinfo.COUNT).setLength(statisticinfo.LENGTH).setCategory(entry.getKey()).build());
        }

        done.run(builder.build());


    }

    @Override
    public Service getService() {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        // TODO Auto-generated method stub
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment arg0) throws IOException {
        // TODO Auto-generated method stub

    }


    private static String GetCategoryCode(String rowcode){
        return rowcode.substring(12, 16);
    }

    private Filter GetwktColumnFilter(){
        List<Filter> columnFilters = new ArrayList<Filter>();
        columnFilters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("Geometry"))));
        columnFilters.add(new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("CFStatIndex"))));
        FilterList columnFiltersList = new FilterList(Operator.MUST_PASS_ONE,columnFilters);
        return columnFiltersList;
    }

}
