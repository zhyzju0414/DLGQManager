package edu.zju.gis.hbase.query;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.esri.core.geometry.Wkid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import com.esri.core.geometry.Point;

import edu.zju.gis.hbase.entity.CategoryStatisticInfo;
import edu.zju.gis.hbase.entity.FeatureClassObj;
import edu.zju.gis.hbase.entity.FeatureClassObjList;
import edu.zju.gis.hbase.entity.KeyWordQueryObjList;
import edu.zju.gis.hbase.entity.Position;
import edu.zju.gis.hbase.entity.PositionList;
import edu.zju.gis.hbase.query.querycondition.KeyWordPropertyQueryCondition;
import edu.zju.gis.hbase.query.querycondition.PropertyQueryCondition;
import edu.zju.gis.hbase.query.querycondition.QueryCondition;
import edu.zju.gis.hbase.query.querycondition.QueryConditionFactory;
import edu.zju.gis.hbase.query.querycondition.SpatialQueryCondition;
import edu.zju.gis.hbase.statistic.QueryStatisticClass;
import edu.zju.gis.hbase.statistic.SummaryQueryStatisticClass;
import edu.zju.gis.hbase.tool.BatchGetServer;
import edu.zju.gis.hbase.tool.HTablePool;
import edu.zju.gis.hbase.tool.HashGetTaskSpliter;
import edu.zju.gis.hbase.tool.IGetTaskSpliter;
import edu.zju.gis.hbase.tool.IResultProcessor;
import edu.zju.gis.hbase.tool.PointContainFilter;
import edu.zju.gis.hbase.tool.PointQueryCondition;
import edu.zju.gis.hbase.tool.RawResultProcessor;
import edu.zju.gis.hbase.tool.Utils;

/*
 * 矢量数据查询统计服务接口
 */
public class FeatureQueryStatisticService implements IFeatureQueryService {

    private IndexSearcher indexSearcher;  //基于lucene的索引查询对象
    private HTablePool datatablePool;        //数据表连接池
    private HTablePool indexdatatablePool;   //索引表连接池
    private String tableName;

    private CacheManager cacheManager;      //数据缓存管理器
    Configuration conf;

    public FeatureQueryStatisticService(String tableName){

        this.conf = HBaseConfiguration.create();
        this.datatablePool = new HTablePool(10, conf, tableName);
        this.indexdatatablePool = new HTablePool(10, conf, Utils.GetSpatialIndexTableNameByDataTableName(tableName));
        this.tableName = tableName;
        cacheManager = new CacheManager();
    }

    public FeatureQueryStatisticService(String tableName,String luceneindexPath) throws IOException{

        Configuration conf = HBaseConfiguration.create();
        this.datatablePool = new HTablePool(10, conf, tableName);
        this.indexdatatablePool = new HTablePool(10, conf, Utils.GetSpatialIndexTableNameByDataTableName(tableName));
        this.tableName = tableName;

        Directory indexdirectory;
        indexdirectory =FSDirectory.open(new File(luceneindexPath).toPath());
        IndexReader indexReader = DirectoryReader.open(indexdirectory);
        indexSearcher = new IndexSearcher(indexReader);

        cacheManager = new CacheManager();

    }




    public Configuration getConf() {
        return conf;
    }

    @Override
    public FeatureClassObj GetFeatureInfo(String rowcode) {
        // TODO Auto-generated method stub
        System.out.println("GetFeatureInfo:"+rowcode);
        Get get = new Get(Bytes.toBytes(rowcode));
        try {
            Result result = datatablePool.GetTableInstance().get(get);
            return FeatureClassObj.ConstructFeatureClassObj(result);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }



    @Override
    public PositionList GetFeatureList(Map<String, String> queryParameter) {
        // TODO Auto-generated method stub
        QueryCondition qc = QueryConditionFactory.CreateQueryCondition(queryParameter);
        System.out.println("GetFeatureList:"+qc.toString());
        //如果只做统计
        if(QueryConditionFactory.StatisticOnly(queryParameter)){
            CategoryStatisticInfo ststisticInfo = GetStatisticInfo(queryParameter);
            PositionList pl = new PositionList();
            pl.setStatisticInfo(ststisticInfo);
            return pl;
        }
        //首先在缓存容器中查找结果是否存在，若存在，直接取出返回 不存在，只能进行查询了
        PositionList data = cacheManager.GetData(qc);
        if(data!=null){
            return data;
        }else{
            IFeatureQueryable queryObj = null;
            data = new PositionList();

            if(qc instanceof SpatialQueryCondition){//空间查询
                queryObj = new SpatialQueryClass(datatablePool,indexdatatablePool);
            }else if(qc instanceof PropertyQueryCondition){ //属性查询
                if(qc instanceof KeyWordPropertyQueryCondition){  //关键字查询
                    queryObj = new KeyWordPropertyQueryClass(datatablePool,indexSearcher);
                }else{
                    queryObj = new PropertyQueryClass(datatablePool);
                }
            }
            if(queryObj!=null){
                try {
                    //获取查询结果
                    List<Result> results = queryObj.DoQuery(qc, QueryFilterManager.GetPositionListFilter());
                    System.out.println("resultsize= "+results.size());
                    for(Result re :results){
//                        System.out.println("result: "+ re.toString());
                    }

                    //进行统计分析,暂时只考虑汇总统计
                    QueryStatisticClass statisticObj = new SummaryQueryStatisticClass();
                    CategoryStatisticInfo statisticInfo = statisticObj.DoStatistic(results,qc.getCategoryCode());
                    data.setStatisticInfo(statisticInfo);
                    for(Result rs:results){
                        data.AddPosition(Position.ConstructPosition(rs));
                    }
                    //缓存查询结果
                    cacheManager.CacheResult(qc, data);
                    return data;
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        return null;
    }



    @Override
    public FeatureClassObjList GetFeatureListByPage(
            Map<String, String> queryParameter) {
        // 首先在缓存中取出指定记录的ID,然后get到要素的详细信息
        FeatureClassObjList obj = new FeatureClassObjList();

        int startindex = 0;
        int length = 10;
        if(queryParameter.containsKey(QueryConditionFactory.startindexParameterName)){
            startindex = Integer.parseInt(queryParameter.get(QueryConditionFactory.startindexParameterName));
        }
        if(queryParameter.containsKey(QueryConditionFactory.lengthParameterName)){
            length = Integer.parseInt(queryParameter.get(QueryConditionFactory.lengthParameterName));
        }

        QueryCondition qc = QueryConditionFactory.CreateQueryCondition(queryParameter);
        System.out.println("GetFeatureListByPage:"+qc.toString());

        PositionList pl = cacheManager.GetData(qc, startindex, length);
        if(pl==null){
            //缓存中不存在，则重新查询
            GetFeatureList(queryParameter);
            pl = cacheManager.GetData(qc, startindex, length);
            if(pl==null){
                //证明该查询对象超过缓存容量，无法缓存
                return null;
            }
        }
        List<Get> getList = new ArrayList<Get>();
        Filter filter = QueryFilterManager.GetDetailFeatureFilter();
        for(int i=0;i<pl.getPositionList().size();i++){
            Get get = new Get(Bytes.toBytes(pl.getPositionList().get(i).ROWCODE));
            get.setFilter(filter);
            getList.add(get);
        }
        try {
            Result[] results = datatablePool.GetTableInstance().get(getList);
            for(Result result:results){
                if(!result.isEmpty()){
                    obj.AddFeatureClassObj(FeatureClassObj.ConstructFeatureClassObj(result));
                }
            }
            return obj;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
    }



    @Override
    public CategoryStatisticInfo GetStatisticInfo(
            Map<String, String> queryParameter) {
        // TODO Auto-generated method stub
        QueryCondition qc = QueryConditionFactory.CreateQueryCondition(queryParameter);

        System.out.println("GetStatisticInfo:"+qc.toString());
        IFeatureQueryable queryObj = null;

        if(qc instanceof SpatialQueryCondition){//空间查询
            queryObj = new SpatialQueryClass(datatablePool,indexdatatablePool);
        }else if(qc instanceof PropertyQueryCondition){ //属性查询
            queryObj = new PropertyQueryClass(datatablePool);
        }else if(qc instanceof KeyWordPropertyQueryCondition){  //关键字查询
            queryObj = new KeyWordPropertyQueryClass(datatablePool,indexSearcher);
        }
        if(queryObj!=null){
            try {
                return queryObj.DoStatistics(qc, "");
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return null;
    }



    @Override
    public FeatureClassObj GetPointQueryFeature(Map<String, String> queryParameter) {
        // TODO Auto-generated method stub
        double x,y;
        if(!(queryParameter.containsKey(QueryConditionFactory.xParameterName)&&queryParameter.containsKey(QueryConditionFactory.yParameterName))){
            return null;
        }
        x = Double.parseDouble(queryParameter.get(QueryConditionFactory.xParameterName));
        y = Double.parseDouble(queryParameter.get(QueryConditionFactory.yParameterName));

        System.out.println("GetPointQueryFeature:x: "+x+" y:"+y);

        PointQueryCondition pqc = new PointQueryCondition(x,y);
        PointContainFilter pcf = new PointContainFilter(x, y);
        //计算和这个点相交的格网
        List<Long> grids = Utils.GetContainGrid(new Point(x,y));
        for(int i=grids.size()-1;i>=0;i--){
            try {
                Result res= indexdatatablePool.GetTableInstance().get(new Get(Bytes.toBytes(grids.get(i).toString())));
                List<Get> getdata = new ArrayList<Get>();
                if(!res.isEmpty()){
                    for (Cell cell : res.rawCells()) {
                        Get g = new Get(Bytes.copy(cell.getValueArray(),
                                cell.getValueOffset(), cell.getValueLength()));
                        g.setFilter(pcf);
                        getdata.add(g);
                    }
                    IGetTaskSpliter hgs = new HashGetTaskSpliter(getdata,1);
                    IResultProcessor rp = new RawResultProcessor();
                    BatchGetServer bgs = new BatchGetServer<Result>(hgs, datatablePool, rp);
                    List<Result> result = bgs.ParallelBatchGet();
                    if(result.size()>0)
                        return FeatureClassObj.ConstructFeatureClassObj(result.get(0));
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return null;
    }


    public KeyWordQueryObjList DoKeyWordQuery(Map<String, String> queryParameter) throws Exception{
        QueryCondition qc = QueryConditionFactory.CreateQueryCondition(queryParameter);

        System.out.println("DoKeyWordQuery:"+qc.toString());

        if(qc instanceof KeyWordPropertyQueryCondition){  //关键字查询
            KeyWordPropertyQueryClass queryObj = new KeyWordPropertyQueryClass(datatablePool,indexSearcher);
            return queryObj.DoKeyWordQuery(qc);
        }
        return null;
    }

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum",
//                "namenode,datanode1,datanode2");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("","");
//        testGetPointQueryFeature();
        testGetFeatureList();
        //testGetFeatureInfo();

        //testGetFeatureListByPage();
        //	testGetStatisticInfo();
    }

      public static String testwkt ="POLYGON((119.98138895471195 30.491716427636714,120.0170945211182 30.60020641787109,119.97177591760257 30.522615475488276,119.90860453088382 30.63591198427734,119.87289896447757 30.527421994042964,119.96559610803226 30.410005612207026,119.98138895471195 30.491716427636714))";
//      public static String testwkt="POLYGON((116.41465187072754 39.92266416549683,116.41533851623535 39.92026090621949,116.41302108764648 39.919660091400154,116.41091823577881 39.9209475517273,116.41130447387695 39.92292165756226,116.41465187072754 39.92266416549683))";
//      public static String testwkt="POLYGON((116.21749877929688 40.066795349121094,116.21097564697266 40.010833740234375,116.2686538696289 39.9294662475586,116.32495880126953 40.00568389892579,116.2624740600586 40.05649566650391,116.21749877929688 40.066795349121094))";
//        public static String testwkt="POLYGON((116.38111352920532 39.91584062576294,116.38261556625366 39.914681911468506,116.38188600540161 39.91260051727295,116.38104915618896 39.91339445114136,116.38042688369751 39.91440296173096,116.38111352920532 39.91584062576294))";
    /*
     * success
     */
    public static void testGetFeatureInfo(){
        FeatureQueryStatisticService service = new FeatureQueryStatisticService("LCRA");
        FeatureClassObj obj = service.GetFeatureInfo("330185000000025000003863");

        System.out.println(obj);
    }


    /*
     * success
     */
    public static void testGetFeatureList(){
        FeatureQueryStatisticService service = new FeatureQueryStatisticService("LCRA2");
        double tolerance = Wkid.find_tolerance_from_wkid(4490);
        System.out.println("tolerance:　"+tolerance);
        Map<String, String> queryParameter = new HashMap<String, String>();
//        queryParameter.put("REGIONCODE", "110000");
        queryParameter.put("GEOWKT", testwkt);
        queryParameter.put("CATEGORYCODE", "0000");
        queryParameter.put("STATISTICONLY", "true");
        PositionList pl = service.GetFeatureList(queryParameter);
        for(int i=0;i<pl.getPositionList().size();i++){
//            if(pl.getPositionList().get(i).ROWCODE.contains("114")){
//                System.out.println(pl.getPositionList().get(i));
//            }
            System.out.println(pl.getPositionList().get(i));
        }
        System.out.println(pl.getStatisticInfo());
    }

    /*
     * success
     */
    public static void testGetPointQueryFeature(){
        com.esri.core.geometry.Wkid wkid = new Wkid();
//        for(int i=0;i<7;i++){
            double tolerance = Wkid.find_tolerance_from_wkid(4490);
            System.out.println("tolerance:　"+tolerance);
//        }
        System.out.println("已初始化wkid");
        FeatureQueryStatisticService service = new FeatureQueryStatisticService("LCRA2");

        Map<String, String> queryParameter = new HashMap<String, String>();
        queryParameter.put("X", "116.53335571289062");
        queryParameter.put("Y", "39.942169189453125");

        FeatureClassObj obj = service.GetPointQueryFeature(queryParameter);

        System.out.println(obj);
    }


    /*
     * success
     */
    public static void testGetFeatureListByPage(){
        FeatureQueryStatisticService service = new FeatureQueryStatisticService("LCRA");

        Map<String, String> queryParameter = new HashMap<String, String>();
        queryParameter.put("GEOWKT", testwkt);
        queryParameter.put("CATEGORYCODE", "01\t02");
        //queryParameter.put("STARTINDEX", "-1");
        //queryParameter.put("LENGTH", "5");

        FeatureClassObjList list = service.GetFeatureListByPage(queryParameter);
        for(int i=0;i<list.getFeatureClassObjList().size();i++){
            System.out.println(list.getFeatureClassObjList().get(i));
        }
        Map<String, String> queryParameter1 = new HashMap<String, String>();
        queryParameter1.put("GEOWKT", testwkt);
        queryParameter1.put("CATEGORYCODE", "01\t02");
        queryParameter1.put("STARTINDEX", "2");
        queryParameter1.put("LENGTH", "20");
        System.out.println("hh");
        FeatureClassObjList list1 = service.GetFeatureListByPage(queryParameter1);
        for(int i=0;i<list1.getFeatureClassObjList().size();i++){
            System.out.println(list1.getFeatureClassObjList().get(i));
        }


    }

    public static void testGetStatisticInfo(){
        FeatureQueryStatisticService service = new FeatureQueryStatisticService("LCRA");

        Map<String, String> queryParameter = new HashMap<String, String>();
        queryParameter.put("GEOWKT", testwkt);
        queryParameter.put("CATEGORYCODE", "00");

        CategoryStatisticInfo info = service.GetStatisticInfo(queryParameter);
        System.out.println(info);
    }






}
