package edu.zju.gis.hbase.featurequeryservice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;

import com.esri.core.geometry.Point;
import com.google.protobuf.ServiceException;

import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticRequest;
import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticResponse;
import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticService;
import edu.zju.gis.hbase.entity.CategoryStatisticInfo;
import edu.zju.gis.hbase.entity.FeatureClassObj;
import edu.zju.gis.hbase.entity.FeatureClassObjList;
import edu.zju.gis.hbase.entity.Position;
import edu.zju.gis.hbase.entity.PositionList;
import edu.zju.gis.hbase.entity.QueryParameter;
import edu.zju.gis.hbase.entity.StatisticInfo;
import edu.zju.gis.hbase.query.FeatureQueryStatisticService;
import edu.zju.gis.hbase.query.IFeatureQueryable;
import edu.zju.gis.hbase.query.KeyWordPropertyQueryClass;

import edu.zju.gis.hbase.query.QueryFilterManager;
import edu.zju.gis.hbase.query.SpatialQueryClass;
import edu.zju.gis.hbase.query.querycondition.KeyWordPropertyQueryCondition;
import edu.zju.gis.hbase.query.querycondition.PropertyQueryCondition;
import edu.zju.gis.hbase.query.querycondition.QueryConditionFactory;
import edu.zju.gis.hbase.query.querycondition.SpatialQueryCondition;
import edu.zju.gis.hbase.statistic.QueryStatisticClass;
import edu.zju.gis.hbase.statistic.SummaryQueryStatisticClass;
import edu.zju.gis.hbase.tool.BatchGetServer;
import edu.zju.gis.hbase.tool.BatchScanServer;
import edu.zju.gis.hbase.tool.CategoryManager;
import edu.zju.gis.hbase.tool.HTablePool;
import edu.zju.gis.hbase.tool.HashGetTaskSpliter;
import edu.zju.gis.hbase.tool.IGetTaskSpliter;
import edu.zju.gis.hbase.tool.IResultProcessor;
import edu.zju.gis.hbase.tool.IScanTaskSpliter;
import edu.zju.gis.hbase.tool.PointContainFilter;
import edu.zju.gis.hbase.tool.PointQueryCondition;
import edu.zju.gis.hbase.tool.QueryCondition;
import edu.zju.gis.hbase.tool.RawResultProcessor;
import edu.zju.gis.hbase.tool.RegionCategoryBasedScanTaskSpliter;
import edu.zju.gis.hbase.tool.RegionManager;
import edu.zju.gis.hbase.tool.Utils;
import edu.zju.gis.hbase.tool.Utils.StatisticIndexEnum;

public class LcraQueryService {

    //地表覆盖数据查询服务
    private FeatureQueryStatisticService lcraservice;
    private HTable gncfTable;

    public LcraQueryService(String lcraTableName,String gncfTableName) {
        super();
        lcraservice = new FeatureQueryStatisticService(lcraTableName);
        try {
            gncfTable = new HTable(lcraservice.getConf(), gncfTableName);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public FeatureClassObj GetFeatureInfo(String rowcode) {
        // TODO Auto-generated method stub
        return lcraservice.GetFeatureInfo(rowcode);
    }

    public PositionList GetFeatureList(Map<String, String> queryParameter) {
        // TODO Auto-generated method stub

        queryParameter = QueryParameterTransfer(queryParameter);
//        PositionList list =lcraservice.GetFeatureList(queryParameter);
//        return list;
        return lcraservice.GetFeatureList(queryParameter);

    }

    public FeatureClassObjList GetFeatureListByPage(
            Map<String, String> queryParameter) {
        queryParameter = QueryParameterTransfer(queryParameter);
        return lcraservice.GetFeatureListByPage(queryParameter);
    }

    public CategoryStatisticInfo GetStatisticInfo(
            Map<String, String> queryParameter) {
        // TODO Auto-generated method stub
        queryParameter = QueryParameterTransfer(queryParameter);
        return lcraservice.GetStatisticInfo(queryParameter);
    }

    public FeatureClassObj GetPointQueryFeature(Map<String, String> queryParameter) {
        // TODO Auto-generated method stub
        return lcraservice.GetPointQueryFeature(queryParameter);
    }

    /*
     * 对查询参数进行转化,转化为基于行政区或者空间的查询
     * 这一步只是为了处理基于地理单元查询地表覆盖要素这一请求
     * 仅仅处理queryParameters里面含有gncrowkey这个查询条件的查询
     */
    public Map<String, String> QueryParameterTransfer(Map<String, String> queryParameter){
        // TODO Auto-generated method stub

        try {
            if(!queryParameter.containsKey(QueryConditionFactory.gncRowKeyParameterName)){
                return queryParameter;
            }

            String rowKey = queryParameter.get(QueryConditionFactory.gncRowKeyParameterName);
            String category = Utils.GetCategoryCode(rowKey);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result res;

            //如果分类为省、市、县行政区，则直接进行属性查询，否则进行空间查询
            if(category.equals("1112")||category.equals("1114")||category.equals("1115")){
                get.setFilter(QueryFilterManager.GetDetailFeatureFilter());
                res = gncfTable.get(get);
                FeatureClassObj obj = FeatureClassObj.ConstructFeatureClassObj(res);
                String regionCode = obj.PROPERTY.get("PAC");
                queryParameter.put(QueryConditionFactory.regionCodeParameterName, regionCode);
//                queryParameter.put(QueryConditionFactory.regionCodeParameterName, "330206");


            }else{ //进行空间查询
                res = gncfTable.get(get);
                FeatureClassObj obj = FeatureClassObj.ConstructFeatureClassObj(res);
                queryParameter.put(QueryConditionFactory.geoWKTParameterName, obj.GEOMETRYWKT);
            }
            return queryParameter;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }



    public static void testGetFeatureListByGraphicUnit(){

        LcraQueryService service = new LcraQueryService("LCRA2", "GNCFA");
        Date d1 = new Date();
        Map<String, String> queryParameter = new HashMap<String, String>();
//        queryParameter.put("gncrowkey".toUpperCase(), "330000000000111400000004");
//        queryParameter.put("categorycode".toUpperCase(), "0110");
//        queryParameter.put("STATISTICONLY", "true");
        queryParameter.put("categorycode".toUpperCase(), "0000");
        queryParameter.put("gncrowkey".toUpperCase(), "110000000000111200000019");
//        queryParameter.put("gncrowkey".toUpperCase(),"220000000000111200000192");
//        queryParameter.put("gncrowkey".toUpperCase(),"330000000000111200000174");
//        queryParameter.put("gncrowkey".toUpperCase(),"510000000000111200000026");
//        queryParameter.put("gncrowkey".toUpperCase(),"330000000000112800000236");
//        queryParameter.put("keyword".toUpperCase(), "");
        queryParameter.put("STATISTICONLY", "true");
        PositionList list = service.GetFeatureList(queryParameter);
        Date d2 = new Date();
        for(int i=0;i<list.getPositionList().size();i++){
            System.out.println(list.getPositionList().get(i));
        }
        System.out.println(list.getStatisticInfo());
        System.out.println(list.getPositionList().size());
        System.out.println(d1);
        System.out.println(d2);
//		queryParameter.put("GEOWKT", testwkt);
//		queryParameter.put("CATEGORYCODE", "01\t02");
    }

    public static void testSpatialGetetFeatureList(){

        LcraQueryService service = new LcraQueryService("LCRA1", "GNCFA");
        Map<String, String> queryParameter = new HashMap<String, String>();
        queryParameter.put("REGIONCODE".toUpperCase(), "330600");
        queryParameter.put("CATEGORYCODE".toUpperCase(), "0000");
        queryParameter.put("STATISTICONLY", "false");
        PositionList info =
                service.GetFeatureList(queryParameter);
        System.out.println(info.getStatisticInfo());
    }

    public static void testGetStatisticInfo()
    {
        LcraQueryService service = new LcraQueryService("LCRA1", "GNCFA");
        Map<String, String> queryParameter = new HashMap<String, String>();
//        queryParameter.put("geoWKT".toUpperCase(), "POLYGON((120.07956242287791 29.231908139686123,120.08376812661326 29.224119004706875,120.08866047585643 29.229783830146328,120.08735155785716 29.231886682014004,120.08282398904002 29.233388719062344,120.07956242287791 29.231908139686123))");
        queryParameter.put("CategoryCode".toUpperCase(), "0000");
//        queryParameter.put("gncrowkey".toUpperCase(),"110000000000111200000019");
        queryParameter.put("gncrowkey".toUpperCase(),"220000000000111200000192");
        queryParameter.put("STATISTICONLY", "true");
        CategoryStatisticInfo INFO  =service.GetStatisticInfo(queryParameter);
        System.out.println(INFO);
    }


    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        //	testSpatialGetetFeatureList();

        // HBase初始化配置
        Configuration conf = HBaseConfiguration.create();

        // 连接Zookeeper

//        conf.set("hbase.zookeeper.quorum",
//                "gis252.gis.zju,gis251.gis.zju,gis100.gis.zju,gis102.gis.zju,gis103.gis.zju,gis104.gis.zju");
            conf.set("hbase.zookeeper.quorum",
                    "namenode,datanode1,datanode2");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
        	testGetFeatureListByGraphicUnit();
//        testSpatialGetetFeatureList();
//        testGetStatisticInfo();
    }


}
