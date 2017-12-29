package edu.zju.gis.hbase.query;

import com.google.protobuf.ServiceException;
import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticRequest;
import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticResponse;
import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticService;
import edu.zju.gis.hbase.entity.CategoryStatisticInfo;
import edu.zju.gis.hbase.entity.StatisticInfo;
import edu.zju.gis.hbase.query.querycondition.PropertyQueryCondition;
import edu.zju.gis.hbase.query.querycondition.QueryCondition;
import edu.zju.gis.hbase.tool.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PropertyQueryClass implements IFeatureQueryable {

    private HTablePool tablePool;

    public PropertyQueryClass(HTablePool tablePool) {
        super();
        this.tablePool = tablePool;
    }


    @Override
    public List<Result> DoQuery(QueryCondition qc, Filter columnFilter) throws Exception {
        // TODO Auto-generated method stub
        if (qc instanceof PropertyQueryCondition) {
            PropertyQueryCondition pqc = (PropertyQueryCondition) qc;
            IScanTaskSpliter hgs = new RegionCategorysBasedScanTaskSpliter(pqc.getRegionCode(), pqc.getCategoryCode(), columnFilter);
            IResultProcessor rp = new RawResultProcessor();
            BatchScanServer bgs = new BatchScanServer<Result>(hgs, tablePool, rp);
            List<Result> result = result = bgs.ParallelBatchScan();

            return result;
        }
        throw new Exception("QueryCondition type error,should be PropertyQueryCondition");
    }


    @Override
    public CategoryStatisticInfo DoStatistics(QueryCondition qc,
                                              String statisticType) throws Exception {
        // TODO Auto-generated method stub
        if (qc instanceof PropertyQueryCondition) {
            PropertyQueryCondition pqc = (PropertyQueryCondition) qc;
            final CategoryStatisticRequest request = pqc.GetCategoryStatisticRequest();

            //若为省级，则不需要起始行键，直接全表扫描，否则设置起始行键

            //协处理器里面的endkey与scan的不一样，scan的endkey是上限为endkey，但不包括endkey，协处理器里面还包括endkey所处的regionserver，所以这里
            //因为数据入库时是以县级数据划分regionserver的，所以在进行县级行政单元的统计时，startkey与endkey设置为一致，做查询是不会导致跨region扫描
            //但是做地市级、或者省级统计时，涉及到跨regionserver统计，endkey设置为下一个区域的起始值，比如330100000000000000000000，下一个区域的
            //起始键为330200000000000000000000，由于330200000000000000000000所处的区间依然在所需统计区域的regionserver中，固不会出错
            //协处理器会扫描指定的某一行所在的region的所有值
//            if (!pqc.getRegionCode().equals(RegionManager.provinceCode)) {

            int regionNum;
            int subRegionNum =100;
            int circleNum;
            String startStr=null;
            String stopStr = null;

            if (true) {
                String[] regionRange = RegionManager.GetRowkeyRange(pqc.getRegionCode());
                startStr = regionRange[0];
                if (RegionManager.GetRegionLevel(pqc.getRegionCode()) == 1){
                    //如果是省的话 判断省下面有多少个region 用协处理器扫描多次
                    regionNum = Utils.REGIONCOUNT.get(Integer.valueOf(pqc.getRegionCode().substring(0,2)));
                    circleNum = (int)Math.ceil((double)regionNum/(double) subRegionNum);
                    System.out.println("regionNum = "+regionNum);
                    System.out.println("circleNum = "+circleNum);
                    stopStr = (Integer.valueOf(regionRange[0].substring(0,2))+1)+"0000000000000000000000";
                }else if (RegionManager.GetRegionLevel(pqc.getRegionCode()) == 2) {
                    //如果是市或者县 就直接扫描
                    circleNum = 1;
                    stopStr = regionRange[1];
                } else {
                    circleNum = 1;
                    stopStr = regionRange[0];
                }
            }

            String[] keysList = new String[circleNum*2];
            if(circleNum==1){
                keysList[0] = startStr;
                keysList[1] = stopStr;
            }else{
                //如果是省的话 通过subRegionNum计算stop key
                keysList[0] = startStr;
                keysList[1]= RegionManager.CountyRegionCode[subRegionNum+Utils.REGIONNUM.get(Integer.valueOf(pqc.getRegionCode().substring(0,2)))-1].toString()+"000000000000";
                for(int i=2;i<circleNum;i+=2){
                    keysList[i*2-2] = RegionManager.CountyRegionCode[(i-1)*subRegionNum+Utils.REGIONNUM.get(Integer.valueOf(pqc.getRegionCode().substring(0,2)))].toString()+"000000000000";
                    keysList[i*2-1] = RegionManager.CountyRegionCode[(i)*subRegionNum+Utils.REGIONNUM.get(Integer.valueOf(pqc.getRegionCode().substring(0,2)))-1].toString()+"000000000000";
                }
                keysList[circleNum*2-2] = RegionManager.CountyRegionCode[(circleNum-1)*subRegionNum+Utils.REGIONNUM.get(Integer.valueOf(pqc.getRegionCode().substring(0,2)))].toString()+"000000000000";
                keysList[circleNum*2-1] = (Integer.valueOf(pqc.getRegionCode().substring(0,2))+1)+"0000000000000000000000";
            }

            Map<String, StatisticInfo> CATEGORYSTATISTICINFO = new HashMap<String, StatisticInfo>();
            StatisticInfo SUMMARY = new StatisticInfo();

            for(int i=0;i<circleNum;i++){
                System.out.println("startkey = "+keysList[i*2]);
                System.out.println("stopkey = "+keysList[i*2+1]);
                byte[] startkey = Bytes.toBytes(keysList[i*2]);
                byte[] stopkey = Bytes.toBytes(keysList[i*2+1]);
                Map<byte[], CategoryStatisticInfo> results=null;
                try{
                    results = tablePool.GetTableInstance().coprocessorService(CategoryStatisticService.class, startkey, stopkey, new Batch.Call<CategoryStatisticService, CategoryStatisticInfo>() {
                        public CategoryStatisticInfo call(CategoryStatisticService counter) throws IOException {
                            ServerRpcController controller = new ServerRpcController();
                            BlockingRpcCallback<CategoryStatisticResponse> rpcCallback =
                                    new BlockingRpcCallback<CategoryStatisticResponse>();
                            System.out.println("========start to scan on regionserver===========");
                            counter.getStatisticInfo(controller, request, rpcCallback);
                            CategoryStatisticResponse response = rpcCallback.get();
                            List<CategoryStatisticResponse.CategoryStatisticInfo> lst = response.getCategoryStatisticInfoList();

                            Map<String, StatisticInfo> CATEGORYSTATISTICINFO = new HashMap<String, StatisticInfo>();
                            StatisticInfo SUMMARY = new StatisticInfo();

                            for (int i = 0; i < lst.size(); i++) {
                                SUMMARY.COUNT += lst.get(i).getCount();
                                SUMMARY.AREA += lst.get(i).getArea();
                                SUMMARY.LENGTH += lst.get(i).getLength();
                                CATEGORYSTATISTICINFO.put(lst.get(i).getCategory(), new StatisticInfo(lst.get(i).getCount(), lst.get(i).getArea(), lst.get(i).getLength()));
                            }
                            CategoryStatisticInfo statisticInfo = new CategoryStatisticInfo(SUMMARY, CATEGORYSTATISTICINFO);
                            return statisticInfo;
//                    }
//                        return null;
                        }
                    });
                }
                catch (Throwable e){
                    e.printStackTrace();
                }
                //获得这一subRegion下面所有region的统计信息
                try {
                    for (Map.Entry<byte[], CategoryStatisticInfo> entry : results.entrySet()) {
                        CategoryStatisticInfo subCategoryStatic = entry.getValue();
                        SUMMARY.AREA += subCategoryStatic.SUMMARY.AREA;
                        SUMMARY.LENGTH += subCategoryStatic.SUMMARY.LENGTH;
                        SUMMARY.COUNT += subCategoryStatic.SUMMARY.COUNT;

                        Map<String, StatisticInfo> map = subCategoryStatic.gotStatisticMap();
                        for (Map.Entry<String, StatisticInfo> ent : map.entrySet()) {
                            if (!CATEGORYSTATISTICINFO.containsKey(ent.getKey())) {
                                CATEGORYSTATISTICINFO.put(ent.getKey(), new StatisticInfo());
                            }
                            CATEGORYSTATISTICINFO.get(ent.getKey()).AREA += ent.getValue().AREA;
                            CATEGORYSTATISTICINFO.get(ent.getKey()).LENGTH += ent.getValue().LENGTH;
                            CATEGORYSTATISTICINFO.get(ent.getKey()).COUNT += ent.getValue().COUNT;
                        }

                      //  System.out.println("。。。。。。。。。。。。。");
                       // System.out.println(SUMMARY.AREA);
                       //System.out.println();
                    }
                }catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            CategoryStatisticInfo statisticinfo = new CategoryStatisticInfo(SUMMARY, CATEGORYSTATISTICINFO);
            statisticinfo.doClassifiedStatistic(CategoryManager.GetTopLevel(pqc.getCategoryCode()));
            return statisticinfo;
        }
//        else {
        throw new Exception("QueryCondition type error,should be PropertyQueryCondition and you can ignore this error");
//            return new CategoryStatisticInfo();
//        }
    }

        public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum",
//        "namenode,datanode1,datanode2");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        args = new String[2];
//        args[0] = "110000";
//        args[1] = "0000";
//        testDoQuery(args[0],args[1]);
        testDoStatistics(args[0],args[1]);

    }

    public static void testDoQuery(String region,String CC) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HTablePool datatablePool = new HTablePool(10, conf, "LCRA1");
        HTablePool indexdatatablePool = new HTablePool(10, conf, Utils.GetSpatialIndexTableNameByDataTableName("LCRA1"));
        PropertyQueryClass sqc = new PropertyQueryClass(datatablePool);
        String[] cats = new String[]{CC};
        Date d1 = new Date();
        PropertyQueryCondition qcon = new PropertyQueryCondition(cats, region);

        List<Result> results = sqc.DoQuery(qcon, QueryFilterManager.GetPositionListFilter());
        Date d2 = new Date();


        for (int i = 0; i < results.size(); i++) {
            System.out.println(Bytes.toString(results.get(i).getRow()));
        }

        System.out.println(d1);
        System.out.println(d2);
        System.out.println(results.size());
    }

    public static void testDoStatistics(String region,String CC) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HTablePool datatablePool = new HTablePool(10, conf, "LCRA2");

        PropertyQueryClass sqc = new PropertyQueryClass(datatablePool);
//        String region = "33";
        String[] cats = new String[]{CC};
        Date d1 = new Date();
        PropertyQueryCondition qcon = new PropertyQueryCondition(cats, region);
        CategoryStatisticInfo statisticInfo = sqc.DoStatistics(qcon, "");

        Date d2 = new Date();
        System.out.println(statisticInfo.SUMMARY);
        for (int i = 0; i < statisticInfo.CATEGORYSTATISTICLIST.size(); i++) {
            System.out.println(statisticInfo.CATEGORYSTATISTICLIST.get(i));
        }
        System.out.println(d1);
        System.out.println(d2);

    }


    public static void testDoQuery1() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HTablePool datatablePool = new HTablePool(10, conf, "GNCFA");
        HTablePool indexdatatablePool = new HTablePool(10, conf, Utils.GetSpatialIndexTableNameByDataTableName("LCRA"));
        PropertyQueryClass sqc = new PropertyQueryClass(datatablePool);
        String region = "330103";
        String[] cats = new String[]{"1114"};
        Date d1 = new Date();
        PropertyQueryCondition qcon = new PropertyQueryCondition(cats, region);

        List<Result> results = sqc.DoQuery(qcon, QueryFilterManager.GetPositionListFilter());
        Date d2 = new Date();


        for (int i = 0; i < results.size(); i++) {
            System.out.println(Bytes.toString(results.get(i).getRow()));
        }

        System.out.println(d1);
        System.out.println(d2);
        System.out.println(results.size());
    }


}
