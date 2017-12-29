package edu.zju.gis.hbase.tool;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import scala.Tuple2;
import scala.Tuple3;

/*
 * 查询条件对象，可以基于行政区、分类要素、空间矩形范围构造hbase查询过滤器
 */
public class QueryCondition implements Serializable{

    public String regionCode;     //行政区代码，12位

    public String categoryCode;   //分类编码，4位

    public Envelope mbr;		 //空间查询范围

    public String timeStamp;    //时间戳

    public Geometry geo;       //查询的几何对象

    public String keyWords;    //搜索关键字

    public String geoStr;



    private boolean spatialQueryInitialized = false;
    private boolean propertyQueryInitialized = false;

    /*
     * 初始化空间查询条件,按照分类、多边形进行查询
     */
    public void InitializeSpatialQueryParameter(String geowkt,String categoryCode) throws Exception{
        this.geoStr = geowkt;
        this.geo = GeometryEngine.geometryFromWkt(geowkt, 0, Utils.getGeometryType("POLYGON"));

        this.categoryCode = categoryCode;
        mbr = new Envelope();
        if(this.geo!=null){
            geo.queryEnvelope(mbr);
        }
        else
            throw new Exception("bad wkt format!");
        spatialQueryInitialized = true;
    }

    /*
     * 初始化属性查询条件,按照分类、行政区进行查询
     */
    public void InitializePropertyQueryParameter(String regionCode,String categoryCode){
        this.regionCode = regionCode;
        this.categoryCode = categoryCode;
        propertyQueryInitialized = true;
    }

    public void InitializePropertyQueryParameter(String regionCode,String categoryCode,String keyWords){
        this.regionCode = regionCode;
        this.categoryCode = categoryCode;
        this.keyWords = keyWords;
        propertyQueryInitialized = true;
    }

    private String getKeyWords() {
        return keyWords;
    }

    private void setKeyWords(String keyWords) {
        this.keyWords = keyWords;
    }

    public String getRegionCode() {
        return QueryCondition.trimEnd(regionCode, '0');
    }

    public String getLongRegionCode(){
        int padLen = 12-regionCode.length();
        String longRegionCode = regionCode;
        while(padLen>0){
            longRegionCode=longRegionCode+'0';
            padLen--;
        }
        return longRegionCode;
    }

    public void setRegionCode(String regionCode) {
        this.regionCode = regionCode;
    }

    public String getCategoryCode() {
        return QueryCondition.trimEnd(categoryCode, '0');
    }

    public void setCategoryCode(String categoryCode) {
        this.categoryCode = categoryCode;
    }

    private String getParentCategoryCode(){
        return this.categoryCode.substring(0, 2);
    }

    private int GetCategoryLevel(){
        int level = 0;
        if(this.categoryCode.endsWith("0")){
            if(this.categoryCode.endsWith("00")){
                return 1;
            }else{
                return 2;
            }
        }else{
            return 3;
        }
    }

    public Envelope getMbr() {
        return mbr;
    }

    public void setMbr(Envelope mbr) {
        this.mbr = mbr;
    }

    /*
     * 根据行政区、分类码获取属性过滤条件,该方法已经舍弃
     */
    public Filter GetPropertyQueryFilter() throws Exception{
        //正则表达式判断行
        if(!propertyQueryInitialized){
            throw new Exception("Initialize property query condition first!");
        }

        String reg = String.format("%s\\d{%d}%s\\d{%d}\\d{8}", getRegionCode(),12-getRegionCode().length(),getCategoryCode(),4-getCategoryCode().length());
        //设置 startrowkey endrowkey
        Filter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator(reg));
        return filter1;
        //不采用正则表达式进行过滤

    }


    /*
     * 获取空间索引搜索条件  1118 zxw
     */
    private Tuple2<List<Scan>,List<Get>> GetSpatialIndexFilter(){
        //类别过滤器
        Filter f =null;
        if(!getParentCategoryCode().startsWith("00")){
            Filter columnfamilyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes(getParentCategoryCode())));
            List<Filter> filterList = new ArrayList<Filter>();
            filterList.add(columnfamilyFilter);
            //根据CC码分类层级进一步判断是否需要进行列过滤
            if(GetCategoryLevel()>1){
                String reg = String.format("\\d{%d}%s\\d{%d}\\d{8}", 12,getCategoryCode(),4-getCategoryCode().length());
                Filter columnFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(reg));
                filterList.add(columnFilter);
            }
            f = new FilterList(Operator.MUST_PASS_ALL,filterList);
        }

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

    //10.31 根据全包含格网生成该格网以及该格网的子格网的搜索Scan  1118
    private List<Scan> GetFilterByAllContainedGrid(Tuple3<Integer, Long, Long> allContainedGrid,Filter otherFilter){
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


    //获取空间数据过滤器
    public List<Get>  GetOptimalSpatialDataFilter(HTablePool indexTable,Filter otherFilter) throws Exception{

        if(!spatialQueryInitialized){
            throw new Exception("Initialize spatial query condition first!");
        }

        List<Get> getList = new ArrayList<Get>();
        Tuple2<List<Scan>,List<Get>> indexfilter = GetSpatialIndexFilter();

        //1118
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

        Filter refinementFilter =  new CustomFilter(Bytes.toBytes(geoStr));
        Filter filter;
        if(otherFilter!=null){
            List<Filter> filterArr = new ArrayList<Filter>();
            filterArr.add(refinementFilter);
            filterArr.add(otherFilter);
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
                if(otherFilter!=null)
                    get.setFilter(filter);
                getList.add(get);
            }
        }
        return getList;
    }


    private static String trimEnd(String str,char c){
        int endindex = str.length()-1;
        while(str.charAt(endindex)==c&&endindex>0){
            endindex--;
        }
        return str.substring(0, endindex+1);
    }

    private static String GetEmptyCategoryCode(){
        return "0000";
    }

}
