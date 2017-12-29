package edu.zju.gis.hbase.query.querycondition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;

import scala.Tuple2;
import scala.Tuple3;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;

import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticRequest;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticRequest.Builder;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticRequest.GetInfo;
import edu.zju.gis.hbase.query.QueryFilterManager;
import edu.zju.gis.hbase.tool.BatchGetServer;
import edu.zju.gis.hbase.tool.BatchScanServer;
import edu.zju.gis.hbase.tool.CustomFilter;
import edu.zju.gis.hbase.tool.HTablePool;
import edu.zju.gis.hbase.tool.HashGetTaskSpliter;
import edu.zju.gis.hbase.tool.IGetTaskSpliter;
import edu.zju.gis.hbase.tool.IResultProcessor;
import edu.zju.gis.hbase.tool.IScanTaskSpliter;
import edu.zju.gis.hbase.tool.ListScanTaskSpliter;
import edu.zju.gis.hbase.tool.RawResultProcessor;
import edu.zju.gis.hbase.tool.Utils;
import edu.zju.gis.hbase.tool.ZCurve;

/*
 * 支持空间、分类要素联合查询
 */
public class SpatialQueryCondition extends QueryCondition {

    private List<Geometry> geos;    //面状要素及外包矩形框集合
    private String[] wktStrs;               //wkt字符串

    public SpatialQueryCondition(String[] categoryCode,String[] wktStrs) {
        super(categoryCode);
        // TODO Auto-generated constructor stub
        this.wktStrs = wktStrs;
        geos = new ArrayList<Geometry>();
        for(int i=0;i<wktStrs.length;i++){
            Geometry geo =  GeometryEngine.geometryFromWkt(wktStrs[i], 0, Utils.getGeometryType("POLYGON"));

            geos.add(geo);
        }
    }

    public List<List<Get>>  GetOptimalSpatialDataFilter(HTablePool indexTable,Filter otherFilter) throws Exception{
        List<List<Get>> list  = new ArrayList<List<Get>>();
        for(int i=0;i<wktStrs.length;i++){
            List<Get> getList = new ArrayList<Get>();
            for(int j=0;j<getCategoryCode().length;j++){
                Tuple2<List<Scan>,List<Get>> indexfilter = GetSpatialIndexFilter(geos.get(i),getCategoryCode()[j]);
                //如果没有全包含关系的格网就不用做下面这步
                IScanTaskSpliter taskSpliter = new ListScanTaskSpliter(indexfilter._1());
                IResultProcessor rp = new RawResultProcessor();
                BatchScanServer scanServer = new BatchScanServer<Result>(taskSpliter, indexTable, rp);
                List<Result> result = scanServer.ParallelBatchScan();
                if(result!=null){
                    for(Result res:result){
                        for(Cell cell : res.rawCells()){
                            Get get = new Get(Bytes.copy(cell.getValueArray(),
                                    cell.getValueOffset(), cell.getValueLength()));
                            if(otherFilter!=null)
                                get.setFilter(otherFilter);
                            getList.add(get);
                        }
                    }
                }

//                System.out.println("getListSize = "+getList.size());   //0

                Filter refinementFilter =  new CustomFilter(Bytes.toBytes(wktStrs[i]));
                Filter filter;
                if(otherFilter!=null){
                    List<Filter> filterArr = new ArrayList<Filter>();
                    filterArr.add(refinementFilter);
                    filterArr.add(otherFilter);    //otherFilter 没有问题
                    filter = new FilterList(FilterList.Operator.MUST_PASS_ALL,filterArr);
                }
                else
                    filter = refinementFilter;
                //并发Get 1117
                IGetTaskSpliter getTaskSpliter = new HashGetTaskSpliter(indexfilter._2(),1);
                IResultProcessor resultProcessor = new RawResultProcessor();
                BatchGetServer getServer = new BatchGetServer<Result>(getTaskSpliter, indexTable, resultProcessor);
                List<Result> getresult = getServer.ParallelBatchGet();
                for(Result res:getresult){
                    for(Cell cell : res.rawCells()){
                        Get get = new Get(Bytes.copy(cell.getValueArray(),
                                cell.getValueOffset(), cell.getValueLength()));

                        //otherFilter 用来获取位置信息 caption 统计信息
                        //但是加了这条过滤器 得到的keyvalue都是none  暂时不用该过滤器
                        //otherFilter size:FilterList AND (2/2): [CustomFilter, FilterList OR (3/3): [QualifierFilter (EQUAL, Position), FamilyFilter (EQUAL, CFStatIndex), QualifierFilter (EQUAL, Caption)]]
                        if(otherFilter!=null){
                            get.setFilter(filter);
//                            System.out.println("otherFilter size:"+ filter.toString());
                        }
                        getList.add(get);
                    }
                }
            }
            list.add(getList);
            for(Get get:getList){
//                System.out.println("通过空间索引表查出来的lcra的rowkey：");
//                System.out.println(new String(get.getRow()));
            }

            System.out.println("通过空间索引获得的get个数为getListSize = "+getList.size());   //7
        }

        return list;
    }

    /*
     * 暂时只考虑一个几何要素的情况,获取空间统计条件
     */
    public SpatialStatisticRequest GetSpatialStatisticRequest(HTablePool indexTable) throws IOException{

        //	final SpatialStatisticRequest request = SpatialStatisticRequest.newBuilder().addGetList(value).setGeowkt(wktStrs[0]).build();
        Builder builder = SpatialStatisticRequest.newBuilder();
        builder.setGeowkt(wktStrs[0]);

        List<GetInfo> getList = new ArrayList<GetInfo>();
        for(int j=0;j<getCategoryCode().length;j++){
            Tuple2<List<Scan>,List<Get>> indexfilter = GetSpatialIndexFilter(geos.get(0),getCategoryCode()[j]);
            IScanTaskSpliter taskSpliter = new ListScanTaskSpliter(indexfilter._1());
            IResultProcessor rp = new RawResultProcessor();
            BatchScanServer scanServer = new BatchScanServer<Result>(taskSpliter, indexTable, rp);
            List<Result> result = scanServer.ParallelBatchScan();
            if(result!=null){
                for(Result res:result){
                    for(Cell cell : res.rawCells()){
                        getList.add(GetInfo.newBuilder().setCliprequired(false).setRowkey(Bytes.toString(Bytes.copy(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength()))).build());
                    }
                }
            }

            //并发Get 1117
            IGetTaskSpliter getTaskSpliter = new HashGetTaskSpliter(indexfilter._2(),1);
            IResultProcessor resultProcessor = new RawResultProcessor();
            BatchGetServer getServer = new BatchGetServer<Result>(getTaskSpliter, indexTable, resultProcessor);
            List<Result> getresult = getServer.ParallelBatchGet();
            for(Result res:getresult){
                for(Cell cell : res.rawCells()){
                    getList.add(GetInfo.newBuilder().setCliprequired(true).setRowkey(Bytes.toString(Bytes.copy(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength()))).build());
                }
            }
        }

        return builder.addAllGetList(getList).build();
    }


    public List<List<Tuple2<byte[],Integer>>> GetOptimalSpatialDataRowKey(HTablePool indexTable) throws Exception{
        List<List<Tuple2<byte[],Integer>>> list  = new ArrayList<List<Tuple2<byte[],Integer>>>();
        for(int i=0;i<wktStrs.length;i++){
            for(int j=0;j<getCategoryCode().length;j++){
                List<Tuple2<byte[],Integer>> rowkeyList = new ArrayList<Tuple2<byte[],Integer>>();
                Tuple2<List<Scan>,List<Get>> indexfilter = GetSpatialIndexFilter(geos.get(i),getCategoryCode()[j]);

                IScanTaskSpliter taskSpliter = new ListScanTaskSpliter(indexfilter._1());
                IResultProcessor rp = new RawResultProcessor();
                BatchScanServer scanServer = new BatchScanServer<Result>(taskSpliter, indexTable, rp);
                List<Result> result = scanServer.ParallelBatchScan();
                if(result!=null){
                    for(Result res:result){
                        for(Cell cell : res.rawCells()){
                            rowkeyList.add(new Tuple2<byte[],Integer>(Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()),0));
                        }
                    }
                }
                //并发Get 1117
                IGetTaskSpliter getTaskSpliter = new HashGetTaskSpliter(indexfilter._2(),1);
                IResultProcessor resultProcessor = new RawResultProcessor();
                BatchGetServer getServer = new BatchGetServer<Result>(getTaskSpliter, indexTable, resultProcessor);
                List<Result> getresult = getServer.ParallelBatchGet();
                for(Result res:getresult){
                    for(Cell cell : res.rawCells()){
                        rowkeyList.add(new Tuple2<byte[],Integer>(Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()),1));
                    }
                }
                list.add(rowkeyList);
            }
        }
        return list;
    }

    /*
     * 获取空间索引搜索条件  1118 zxw
     */
    private static Tuple2<List<Scan>,List<Get>> GetSpatialIndexFilter(Geometry geo,String categoryCode){
        //类别过滤器
        Filter f = QueryFilterManager.GetSpatialIndexCategoryFilter(categoryCode);

        List<Scan> allContainScanList = new ArrayList<Scan>();
        List<Get> boundarygetList = new ArrayList<Get>();
        List<Tuple2<Tuple3<Integer, Long, Long>, Boolean>> gridArr = Utils.GetIntersectGrid(Utils.UNIVERSEGRID, geo, Utils.ENDLEVEL);
        for(int i=0;i<gridArr.size();i++){

            if(gridArr.get(i)._2()){ //全包含格网
                allContainScanList.addAll(GetFilterByAllContainedGrid(gridArr.get(i)._1(),f));
            }else{
                long zvalue = ZCurve.GetZValue(gridArr.get(i)._1._1(),gridArr.get(i)._1._2(), gridArr.get(i)._1._3());
                Get get = new Get(Bytes.toBytes(Long.toString(zvalue)));
                get.setFilter(f);
                boundarygetList.add(get);
            }
        }

        Tuple2<List<Scan>,List<Get>> indexfilter = new Tuple2<List<Scan>,List<Get>>(allContainScanList,boundarygetList);
        return indexfilter;
    }

    private static List<Scan> GetFilterByAllContainedGrid(Tuple3<Integer, Long, Long> allContainedGrid,Filter otherFilter){
        List<Scan> scanList = new ArrayList<Scan>();
        int j = 0;

        for(int i=allContainedGrid._1();i<=Utils.ENDLEVEL;i++,j++){

            Tuple2<Long,Long> zvalueRange = ZCurve.GetZvalueRange(allContainedGrid, j);
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(zvalueRange._1.toString()));
            scan.setStopRow(Bytes.toBytes(Long.toString((zvalueRange._2+1))));
            scan.setFilter(otherFilter);
            scanList.add(scan);
        }
        return scanList;
    }


    public List<Geometry> getGeos() {
        return geos;
    }

    public String[] getWktStrs() {
        return wktStrs;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Arrays.hashCode(wktStrs);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        SpatialQueryCondition other = (SpatialQueryCondition) obj;
        if (!Arrays.equals(wktStrs, other.wktStrs))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "SpatialQueryCondition [wktStrs=" + Arrays.toString(wktStrs)
                + ", getCategoryCode()=" + Arrays.toString(getCategoryCode())
                + "]";
    }


    /*
     *供Spark调用的方法,根据空间查询条件，找出匹配查询条件的数据的Rowkey，List中的结构为:rowkey  geoid cliprequired
     */
    public List<Tuple2<String,String>> GetOptimalSpatialDataKey(HTablePool indexTable) throws IOException{
        List<Tuple2<String,String>> list = new ArrayList<Tuple2<String,String>>();

        for(int i=0;i<wktStrs.length;i++){
            for(int j=0;j<getCategoryCode().length;j++){
                Tuple2<List<Scan>,List<Get>> indexfilter = GetSpatialIndexFilter(geos.get(i),getCategoryCode()[j]);
                IScanTaskSpliter taskSpliter = new ListScanTaskSpliter(indexfilter._1());
                IResultProcessor rp = new RawResultProcessor();
                BatchScanServer scanServer = new BatchScanServer<Result>(taskSpliter, indexTable, rp);
                List<Result> result = scanServer.ParallelBatchScan();
                if(result!=null){
                    for(Result res:result){
                        for(Cell cell : res.rawCells()){
                            list.add(new Tuple2<String,String>(Bytes.toString(Bytes.copy(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength())),i+Utils.DEFAULT_INPUT_DELIMITER+"false"));
                        }
                    }
                }

                IGetTaskSpliter getTaskSpliter = new HashGetTaskSpliter(indexfilter._2(),1);
                IResultProcessor resultProcessor = new RawResultProcessor();
                BatchGetServer getServer = new BatchGetServer<Result>(getTaskSpliter, indexTable, resultProcessor);
                List<Result> getresult = getServer.ParallelBatchGet();
                for(Result res:getresult){
                    for(Cell cell : res.rawCells()){
                        list.add(new Tuple2<String,String>(Bytes.toString(Bytes.copy(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength())),i+Utils.DEFAULT_INPUT_DELIMITER+"true"));
                    }
                }

            }
        }
        return list;
    }

    public static List<Tuple2<String,String>> GetOptimalSpatialDataKey(HTablePool indexTable,String[] categoryArr,String wkt,String wktname) throws IOException{
        List<Tuple2<String,String>> list = new ArrayList<Tuple2<String,String>>();

        Geometry geo =  GeometryEngine.geometryFromWkt(wkt, 0, Utils.getGeometryType("POLYGON"));
        for(int j=0;j<categoryArr.length;j++){
            Tuple2<List<Scan>,List<Get>> indexfilter = GetSpatialIndexFilter(geo,categoryArr[j]);
            IScanTaskSpliter taskSpliter = new ListScanTaskSpliter(indexfilter._1());
            IResultProcessor rp = new RawResultProcessor();
            BatchScanServer scanServer = new BatchScanServer<Result>(taskSpliter, indexTable, rp);
            List<Result> result = scanServer.ParallelBatchScan();
            if(result!=null){
                for(Result res:result){
                    for(Cell cell : res.rawCells()){
                        list.add(new Tuple2<String,String>(Bytes.toString(Bytes.copy(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength())),wktname+Utils.DEFAULT_INPUT_DELIMITER+"false"));
                    }
                }
            }

            IGetTaskSpliter getTaskSpliter = new HashGetTaskSpliter(indexfilter._2(),1);
            IResultProcessor resultProcessor = new RawResultProcessor();
            BatchGetServer getServer = new BatchGetServer<Result>(getTaskSpliter, indexTable, resultProcessor);
            List<Result> getresult = getServer.ParallelBatchGet();
            for(Result res:getresult){
                for(Cell cell : res.rawCells()){
                    list.add(new Tuple2<String,String>(Bytes.toString(Bytes.copy(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength())),wktname+Utils.DEFAULT_INPUT_DELIMITER+"true"));
                }
            }

        }
        return list;
    }





}
