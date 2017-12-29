//package edu.zju.gis.hbase.query;
//
//import java.io.IOException;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.coprocessor.Batch;
//import org.apache.hadoop.hbase.filter.Filter;
//import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
//import org.apache.hadoop.hbase.ipc.ServerRpcController;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import com.google.protobuf.ServiceException;
//
//import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticRequest;
//import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticResponse;
//import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticService;
//import edu.zju.gis.hbase.entity.CategoryStatisticInfo;
//import edu.zju.gis.hbase.entity.StatisticInfo;
//import edu.zju.gis.hbase.query.querycondition.PropertyQueryCondition;
//import edu.zju.gis.hbase.query.querycondition.QueryCondition;
//import edu.zju.gis.hbase.tool.BatchScanServer;
//import edu.zju.gis.hbase.tool.CategoryManager;
//import edu.zju.gis.hbase.tool.HTablePool;
//import edu.zju.gis.hbase.tool.IResultProcessor;
//import edu.zju.gis.hbase.tool.IScanTaskSpliter;
//import edu.zju.gis.hbase.tool.RawResultProcessor;
//import edu.zju.gis.hbase.tool.RegionCategorysBasedScanTaskSpliter;
//import edu.zju.gis.hbase.tool.RegionManager;
//import edu.zju.gis.hbase.tool.Utils;
//
//public class PropertyQueryClass_zxw implements IFeatureQueryable {
//
//    private HTablePool tablePool;
//
//    public PropertyQueryClass_zxw(HTablePool tablePool) {
//        super();
//        this.tablePool = tablePool;
//    }
//
//
//    @Override
//    public List<Result> DoQuery(QueryCondition qc, Filter columnFilter) throws Exception {
//        // TODO Auto-generated method stub
//        if (qc instanceof PropertyQueryCondition) {
//            PropertyQueryCondition pqc = (PropertyQueryCondition) qc;
//            IScanTaskSpliter hgs = new RegionCategorysBasedScanTaskSpliter(pqc.getRegionCode(), pqc.getCategoryCode(), columnFilter);
//            IResultProcessor rp = new RawResultProcessor();
//            BatchScanServer bgs = new BatchScanServer<Result>(hgs, tablePool, rp);
//            List<Result> result = result = bgs.ParallelBatchScan();
//
//            return result;
//        }
//        throw new Exception("QueryCondition type error,should be PropertyQueryCondition");
//    }
//
//
//    @Override
//    public CategoryStatisticInfo DoStatistics(QueryCondition qc,
//                                              String statisticType) throws Exception {
//        // TODO Auto-generated method stub
//        if (qc instanceof PropertyQueryCondition) {
//            PropertyQueryCondition pqc = (PropertyQueryCondition) qc;
//            final CategoryStatisticRequest request = pqc.GetCategoryStatisticRequest();
//            byte[] startkey = null;
//            byte[] stopkey = null;
//            //若为省级，则不需要起始行键，直接全表扫描，否则设置起始行键
//
//            //协处理器里面的endkey与scan的不一样，scan的endkey是上限为endkey，但不包括endkey，协处理器里面还包括endkey所处的regionserver，所以这里
//            //因为数据入库时是以县级数据划分regionserver的，所以在进行县级行政单元的统计时，startkey与endkey设置为一致，做查询是不会导致跨region扫描
//            //但是做地市级、或者省级统计时，涉及到跨regionserver统计，endkey设置为下一个区域的起始值，比如330100000000000000000000，下一个区域的
//            //起始键为330200000000000000000000，由于330200000000000000000000所处的区间依然在所需统计区域的regionserver中，固不会出错
//            //协处理器会扫描指定的某一行所在的region的所有值
////            if (!pqc.getRegionCode().equals(RegionManager.provinceCode)) {
//
//            if (true) {
//                String[] regionRange = RegionManager.GetRowkeyRange(pqc.getRegionCode());
//                startkey = Bytes.toBytes(regionRange[0]);
//                if (RegionManager.GetRegionLevel(pqc.getRegionCode()) == 1){
//                    stopkey = Bytes.toBytes((Integer.valueOf(regionRange[0].substring(0,2))+1)+"0000000000000000000000");
//                }else if (RegionManager.GetRegionLevel(pqc.getRegionCode()) == 2) {
//                    stopkey = Bytes.toBytes(regionRange[1]);
//                } else {
//                    stopkey = Bytes.toBytes(regionRange[0]);
//                }
//            }
//
//            Map<byte[], CategoryStatisticInfo> results;
//            try {
//                results = tablePool.GetTableInstance().coprocessorService(CategoryStatisticService.class, startkey, stopkey, new Batch.Call<CategoryStatisticService, CategoryStatisticInfo>() {
//                    public CategoryStatisticInfo call(CategoryStatisticService counter) throws IOException {
//                        ServerRpcController controller = new ServerRpcController();
//                        BlockingRpcCallback<CategoryStatisticResponse> rpcCallback =
//                                new BlockingRpcCallback<CategoryStatisticResponse>();
//                        System.out.println("========start to scan on regionserver===========");
//                        counter.getStatisticInfo(controller, request, rpcCallback);
//                        CategoryStatisticResponse response = rpcCallback.get();
//                        List<CategoryStatisticResponse.CategoryStatisticInfo> lst = response.getCategoryStatisticInfoList();
//
//                        Map<String, StatisticInfo> CATEGORYSTATISTICINFO = new HashMap<String, StatisticInfo>();
//                        StatisticInfo SUMMARY = new StatisticInfo();
//
//                        for (int i = 0; i < lst.size(); i++) {
//                            SUMMARY.COUNT += lst.get(i).getCount();
//                            SUMMARY.AREA += lst.get(i).getArea();
//                            SUMMARY.LENGTH += lst.get(i).getLength();
//                            CATEGORYSTATISTICINFO.put(lst.get(i).getCategory(), new StatisticInfo(lst.get(i).getCount(), lst.get(i).getArea(), lst.get(i).getLength()));
//                        }
//                        CategoryStatisticInfo statisticInfo = new CategoryStatisticInfo(SUMMARY, CATEGORYSTATISTICINFO);
//                        return statisticInfo;
////                    }
////                        return null;
//                    }
//                });
//
//                Map<String, StatisticInfo> CATEGORYSTATISTICINFO = new HashMap<String, StatisticInfo>();
//                StatisticInfo SUMMARY = new StatisticInfo();
//
//                for (Map.Entry<byte[], CategoryStatisticInfo> entry : results.entrySet()) {
//                    CategoryStatisticInfo subCategoryStatic = entry.getValue();
//                    SUMMARY.AREA += subCategoryStatic.SUMMARY.AREA;
//                    SUMMARY.LENGTH += subCategoryStatic.SUMMARY.LENGTH;
//                    SUMMARY.COUNT += subCategoryStatic.SUMMARY.COUNT;
//
//                    Map<String, StatisticInfo> map = subCategoryStatic.gotStatisticMap();
//                    for (Map.Entry<String, StatisticInfo> ent : map.entrySet()) {
//                        if (!CATEGORYSTATISTICINFO.containsKey(ent.getKey())) {
//                            CATEGORYSTATISTICINFO.put(ent.getKey(), new StatisticInfo());
//                        }
//                        CATEGORYSTATISTICINFO.get(ent.getKey()).AREA += ent.getValue().AREA;
//                        CATEGORYSTATISTICINFO.get(ent.getKey()).LENGTH += ent.getValue().LENGTH;
//                        CATEGORYSTATISTICINFO.get(ent.getKey()).COUNT += ent.getValue().COUNT;
//                    }
//                }
//
//                CategoryStatisticInfo statisticinfo = new CategoryStatisticInfo(SUMMARY, CATEGORYSTATISTICINFO);
//                statisticinfo.doClassifiedStatistic(CategoryManager.GetTopLevel(pqc.getCategoryCode()));
//                return statisticinfo;
//
//            } catch (ServiceException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            } catch (Throwable e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
//        }
////        else {
//        throw new Exception("QueryCondition type error,should be PropertyQueryCondition and you can ignore this error");
////            return new CategoryStatisticInfo();
////        }
//    }
//
//    public static void main(String[] args) throws Exception {
//        // TODO Auto-generated method stub
//        Configuration conf = HBaseConfiguration.create();
////        conf.set("hbase.zookeeper.quorum",
////        "namenode,datanode1,datanode2");
////        conf.set("hbase.zookeeper.property.clientPort", "2181");
////        args = new String[2];
////        args[0] = "110000";
////        args[1] = "0000";
////        testDoQuery(args[0],args[1]);
//        testDoStatistics(args[0],args[1]);
//
//    }
//
//    public static void testDoQuery(String region,String CC) throws Exception {
//        Configuration conf = HBaseConfiguration.create();
//        HTablePool datatablePool = new HTablePool(10, conf, "LCRA1");
//        HTablePool indexdatatablePool = new HTablePool(10, conf, Utils.GetSpatialIndexTableNameByDataTableName("LCRA1"));
//        PropertyQueryClass_zxw sqc = new PropertyQueryClass_zxw(datatablePool);
//        String[] cats = new String[]{CC};
//        Date d1 = new Date();
//        PropertyQueryCondition qcon = new PropertyQueryCondition(cats, region);
//
//        List<Result> results = sqc.DoQuery(qcon, QueryFilterManager.GetPositionListFilter());
//        Date d2 = new Date();
//
//
//        for (int i = 0; i < results.size(); i++) {
//            System.out.println(Bytes.toString(results.get(i).getRow()));
//        }
//
//        System.out.println(d1);
//        System.out.println(d2);
//        System.out.println(results.size());
//    }
//
//    public static void testDoStatistics(String region,String CC) throws Exception {
//        Configuration conf = HBaseConfiguration.create();
//        HTablePool datatablePool = new HTablePool(10, conf, "LCRA2");
//
//        PropertyQueryClass_zxw sqc = new PropertyQueryClass_zxw(datatablePool);
////        String region = "33";
//        String[] cats = new String[]{CC};
//        Date d1 = new Date();
//        PropertyQueryCondition qcon = new PropertyQueryCondition(cats, region);
//        CategoryStatisticInfo statisticInfo = sqc.DoStatistics(qcon, "");
//
//        Date d2 = new Date();
//        System.out.println(statisticInfo.SUMMARY);
//        for (int i = 0; i < statisticInfo.CATEGORYSTATISTICLIST.size(); i++) {
//            System.out.println(statisticInfo.CATEGORYSTATISTICLIST.get(i));
//        }
//        System.out.println(d1);
//        System.out.println(d2);
//
//    }
//
//
//    public static void testDoQuery1() throws Exception {
//        Configuration conf = HBaseConfiguration.create();
//        HTablePool datatablePool = new HTablePool(10, conf, "GNCFA");
//        HTablePool indexdatatablePool = new HTablePool(10, conf, Utils.GetSpatialIndexTableNameByDataTableName("LCRA"));
//        PropertyQueryClass_zxw sqc = new PropertyQueryClass_zxw(datatablePool);
//        String region = "330103";
//        String[] cats = new String[]{"1114"};
//        Date d1 = new Date();
//        PropertyQueryCondition qcon = new PropertyQueryCondition(cats, region);
//
//        List<Result> results = sqc.DoQuery(qcon, QueryFilterManager.GetPositionListFilter());
//        Date d2 = new Date();
//
//
//        for (int i = 0; i < results.size(); i++) {
//            System.out.println(Bytes.toString(results.get(i).getRow()));
//        }
//
//        System.out.println(d1);
//        System.out.println(d2);
//        System.out.println(results.size());
//    }
//
//
//}
