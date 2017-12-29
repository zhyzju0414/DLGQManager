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
import org.apache.hadoop.hbase.util.Bytes;

import edu.zju.gis.hbase.entity.CategoryStatisticInfo;
import edu.zju.gis.hbase.entity.FeatureClassObj;
import edu.zju.gis.hbase.entity.FeatureClassObjList;
import edu.zju.gis.hbase.entity.KeyWordQueryObjList;
import edu.zju.gis.hbase.entity.PositionList;
import edu.zju.gis.hbase.entity.QueryParameter;
import edu.zju.gis.hbase.query.FeatureQueryStatisticService;
import edu.zju.gis.hbase.query.KeyWordPropertyQueryClass;
import edu.zju.gis.hbase.query.QueryFilterManager;
import edu.zju.gis.hbase.query.querycondition.KeyWordPropertyQueryCondition;
import edu.zju.gis.hbase.query.querycondition.QueryCondition;
import edu.zju.gis.hbase.query.querycondition.QueryConditionFactory;
import edu.zju.gis.hbase.tool.HTablePool;
import edu.zju.gis.hbase.tool.Utils;

/*
 * 地理国情要素查询
 */
public class GncFeatureQueryService {

    //地理国情要素查询
    private FeatureQueryStatisticService gncfaservice;
    private HTable gncfTable;

    public GncFeatureQueryService(String gncfTableName,String lucenePath) {
        super();
        try {
            gncfaservice = new FeatureQueryStatisticService(gncfTableName,lucenePath);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public FeatureClassObj GetFeatureInfo(String rowcode) {
        // TODO Auto-generated method stub
        return gncfaservice.GetFeatureInfo(rowcode);
    }

    public PositionList GetFeatureList(Map<String, String> queryParameter) {
        // TODO Auto-generated method stub

        return gncfaservice.GetFeatureList(queryParameter);

    }

    public FeatureClassObjList GetFeatureListByPage(
            Map<String, String> queryParameter) {

        return gncfaservice.GetFeatureListByPage(queryParameter);
    }

    public CategoryStatisticInfo GetStatisticInfo(
            Map<String, String> queryParameter) {
        // TODO Auto-generated method stub
        return gncfaservice.GetStatisticInfo(queryParameter);
    }

    public KeyWordQueryObjList DoKeyWordQuery(Map<String, String> queryParameter) throws Exception{
        return gncfaservice.DoKeyWordQuery(queryParameter);
    }

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = HBaseConfiguration.create();
        HTablePool datatablePool = new HTablePool(3, conf, "GNCFA");
        conf.set("hbase.zookeeper.quorum",
                "namenode,datanode1,datanode2");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
//			KeyWordPropertyQueryClass sqc = new KeyWordPropertyQueryClass(datatablePool,indexSearcher);
//			String region = "33";
//        String[] cats = new String[]{"1112"};
//			Date d1 = new Date();
//			KeyWordPropertyQueryCondition qcon = new KeyWordPropertyQueryCondition(cats, null,"德清");
//        Map<String, String> queryParameter = new HashMap<String, String>();
//        queryParameter.put("KEYWORD", "杭州");
//        queryParameter.put("CATEGORYCODE", "1115");
//        GncFeatureQueryService service = new GncFeatureQueryService("GNCFA", "\\\\10.2.35.7\\share\\HbaseData\\luceneindex_all2");
//        KeyWordQueryObjList lst = service.DoKeyWordQuery(queryParameter);
//
//        for(int i=0;i<lst.OBJLIST.size();i++){
//            Get get = new Get(Bytes.toBytes(lst.OBJLIST.get(i).ROWCODE));
//            Result res = datatablePool.GetTableInstance().get(get);
//            FeatureClassObj fco = FeatureClassObj.ConstructFeatureClassObj(res);
//
//            System.out.println(fco.PROPERTY.get("PAC")+" "+lst.OBJLIST.get(i));
//        }
        	testGetFeatureList();
        //	Get();
    }

    public static void testGetFeatureList(){
        Map<String, String> queryParameter = new HashMap<String, String>();
        //	queryParameter.put("KEYWORD", "场");
        queryParameter.put("CATEGORYCODE", "1115");
        queryParameter.put("REGIONCODE", "110000000000");
        queryParameter.put("STATISTICONLY","true");
//        GncFeatureQueryService service = new GncFeatureQueryService("GNCFA", "\\\\10.2.35.7\\share\\HbaseData\\luceneindex_all2");
        GncFeatureQueryService service = new GncFeatureQueryService("GNCFA", "/home/dlgqjc/share/HbaseData/luceneindex_all2");
        PositionList pl = service.GetFeatureList(queryParameter);

        for(int i=0;i<pl.getPositionList().size();i++){
            System.out.println(pl.getPositionList().get(i));
        }

    }

    public static void Delete(){
        String[] row = new String[]{"330000000000POLYGON00000001"};

        Configuration conf = HBaseConfiguration.create();
        HTablePool datatablePool = new HTablePool(10, conf, "GNCFA");
        List<org.apache.hadoop.hbase.client.Delete> deleteList = new ArrayList<org.apache.hadoop.hbase.client.Delete>();
        for(int i=0;i<row.length;i++){
            deleteList.add(new org.apache.hadoop.hbase.client.Delete(Bytes.toBytes(row[i])));
        }

        try {
            datatablePool.GetTableInstance().delete(deleteList);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


    }

    public static void Get(){
        String[] row = new String[]{"330000000000111200000001"};

        Configuration conf = HBaseConfiguration.create();
        HTablePool datatablePool = new HTablePool(10, conf, "GNCFA");



        try {
            Get get = new Get(Bytes.toBytes(row[0]));
            Result res = datatablePool.GetTableInstance().get(get);

            for (Cell cell : res.rawCells()) {
//			      System.out.println("Cell: " + cell +
//			        ", Value: " + Bytes.toString(cell.getValueArray(),
//			        cell.getValueOffset(), cell.getValueLength()));
                System.out.println("Cell: " + Bytes.toString(cell.getFamily()) +":"+Bytes.toString(cell.getQualifier())+
                        ", Value: " + Bytes.toString(cell.getValueArray(),
                        cell.getValueOffset(), cell.getValueLength()));
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


    }



}
