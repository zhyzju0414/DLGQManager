package edu.zju.gis.hbase.query;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;

import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticResponse;
import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticService;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticRequest;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticRequest.GetInfo;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticResponse;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticResponse.SpatialStatisticInfo;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticService;
import edu.zju.gis.hbase.entity.CategoryStatisticInfo;
import edu.zju.gis.hbase.entity.StatisticInfo;
import edu.zju.gis.hbase.query.querycondition.QueryCondition;
import edu.zju.gis.hbase.query.querycondition.SpatialQueryCondition;
import edu.zju.gis.hbase.tool.BatchGetServer;
import edu.zju.gis.hbase.tool.BatchRpcServer;
import edu.zju.gis.hbase.tool.CategoryManager;
import edu.zju.gis.hbase.tool.HTablePool;
import edu.zju.gis.hbase.tool.IGetTaskSpliter;
import edu.zju.gis.hbase.tool.IRequestTaskSpliter;
import edu.zju.gis.hbase.tool.IResultProcessor;
import edu.zju.gis.hbase.tool.ListGetTaskGet;
import edu.zju.gis.hbase.tool.RawResultProcessor;
import edu.zju.gis.hbase.tool.RegionBasedSpatialStatisticTaskSpliter;
import edu.zju.gis.hbase.tool.SpatialStatisticTaskSpliter;
import edu.zju.gis.hbase.tool.Utils;

public class SpatialQueryClass implements IFeatureQueryable {

    private HTablePool datatablePool;
    private HTablePool indexdatatablePool;

    public SpatialQueryClass(HTablePool datatablePool,HTablePool indexdatatablePool) {
        super();
        this.datatablePool = datatablePool;
        this.indexdatatablePool = indexdatatablePool;
    }

    /*
     * 查询
     * (non-Javadoc)
     * @see edu.zju.gis.hbase.query.IFeatureQueryable#DoQuery(edu.zju.gis.hbase.query.querycondition.QueryCondition, org.apache.hadoop.hbase.filter.Filter)
     */
    @Override
    public List<Result> DoQuery(QueryCondition qc,Filter columnFilter) throws Exception {
        // TODO Auto-generated method stub
        if(qc instanceof SpatialQueryCondition){
            SpatialQueryCondition pqc = (SpatialQueryCondition)qc;
            List<List<Get>> getList = pqc.GetOptimalSpatialDataFilter(indexdatatablePool,columnFilter);
            IGetTaskSpliter getTaskSpliter = new ListGetTaskGet(getList);
            //这里将获得的lcra表里的rowkey进行分组  进行多线程查询
            System.out.println("getList  size = "+getTaskSpliter.GetSplitTasks().size());
            IResultProcessor rp = new RawResultProcessor();
            BatchGetServer getServer = new BatchGetServer<Result>(getTaskSpliter, datatablePool, rp);
            List<Result> rs = getServer.ParallelBatchGet();
            return rs;
        }
        throw new Exception("QueryCondition type error,should be SpatialQueryCondition");
    }

    /*
     * (non-Javadoc)统计
     * @see edu.zju.gis.hbase.query.IFeatureQueryable#DoStatistics(edu.zju.gis.hbase.query.querycondition.QueryCondition, java.lang.String)
     */
    @Override
    public CategoryStatisticInfo DoStatistics(QueryCondition qc,
                                              String statisticType) throws Exception {
        // TODO Auto-generated method stub
        if(qc instanceof SpatialQueryCondition){
            SpatialQueryCondition pqc = (SpatialQueryCondition)qc;

            final SpatialStatisticRequest request = pqc.GetSpatialStatisticRequest(indexdatatablePool);

            //并发远程调用
            IRequestTaskSpliter<SpatialStatisticRequest> taskSpliter = new RegionBasedSpatialStatisticTaskSpliter(request);
            RpcCallClass<CategoryStatisticInfo, SpatialStatisticRequest> rpcCall = new SpatialStatisticRpcCallClass(datatablePool);
            BatchRpcServer<CategoryStatisticInfo, SpatialStatisticRequest> server = new BatchRpcServer<CategoryStatisticInfo, SpatialStatisticProtos.SpatialStatisticRequest>(taskSpliter, rpcCall);
            List<Map<byte[],CategoryStatisticInfo>> result = server.ParallelBatchRpc();

            //收集统计结果
            Map<String,StatisticInfo> CATEGORYSTATISTICINFO = new HashMap<String,StatisticInfo>();
            StatisticInfo SUMMARY = new StatisticInfo();
            for(Map<byte[],CategoryStatisticInfo> map:result){
                for(Map.Entry<byte[],CategoryStatisticInfo> entry:map.entrySet()){
                    CategoryStatisticInfo subCategoryStatic = entry.getValue();
                    SUMMARY.AREA += subCategoryStatic.SUMMARY.AREA;
                    SUMMARY.LENGTH += subCategoryStatic.SUMMARY.LENGTH;
                    SUMMARY.COUNT += subCategoryStatic.SUMMARY.COUNT;

                    Map<String,StatisticInfo> categorymap = subCategoryStatic.gotStatisticMap();
                    for(Map.Entry<String,StatisticInfo> ent:categorymap.entrySet()){
                        if(!CATEGORYSTATISTICINFO.containsKey(ent.getKey())){
                            CATEGORYSTATISTICINFO.put(ent.getKey(), new StatisticInfo());
                        }
                        CATEGORYSTATISTICINFO.get(ent.getKey()).AREA += ent.getValue().AREA;
                        CATEGORYSTATISTICINFO.get(ent.getKey()).LENGTH += ent.getValue().LENGTH;
                        CATEGORYSTATISTICINFO.get(ent.getKey()).COUNT += ent.getValue().COUNT;
                    }
                }
            }

            CategoryStatisticInfo ststisticinfo = new CategoryStatisticInfo(SUMMARY,CATEGORYSTATISTICINFO);
            ststisticinfo.doClassifiedStatistic(CategoryManager.GetTopLevel(pqc.getCategoryCode()));
            return ststisticinfo;

        }
        throw new Exception("QueryCondition type error,should be SpatialQueryCondition");
    }

    public byte[][] GetRowKeyRange(SpatialStatisticRequest request){
        byte[][] rowKeyRange = new byte[2][];

        List<GetInfo> getInfoLst = request.getGetListList();
        if(getInfoLst.size()>0){
            rowKeyRange[0]=Bytes.toBytes(getInfoLst.get(0).getRowkey()); //minimum rowkey
            rowKeyRange[1]=Bytes.toBytes(getInfoLst.get(0).getRowkey()); //maxmum rowkey
        }

        for(int i=0;i<getInfoLst.size();i++){
            byte[] rowkey = Bytes.toBytes(getInfoLst.get(i).getRowkey());
            if(Bytes.compareTo(rowKeyRange[0], rowkey)>0){
                rowKeyRange[0] = rowkey;
            }
            if(Bytes.compareTo(rowKeyRange[1], rowkey)<0){
                rowKeyRange[1] = rowkey;
            }
        }
        String upperRowKey = Bytes.toString(rowKeyRange[1]);
        upperRowKey = upperRowKey.substring(0,16)+Long.toString(Long.parseLong(upperRowKey.substring(16, 24))+1);
        rowKeyRange[1] = Bytes.toBytes(upperRowKey);
        return rowKeyRange;
    }

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum",
//        "namenode,datanode1,datanode2");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        testDoQuery();
        testDoStatistics();
//        testBatchRpcCall();
    }

//    public static String testwkt ="POLYGON((119.98138895471195 30.491716427636714,120.0170945211182 30.60020641787109,119.97177591760257 30.522615475488276,119.90860453088382 30.63591198427734,119.87289896447757 30.527421994042964,119.96559610803226 30.410005612207026,119.98138895471195 30.491716427636714))";
//    public static String testwkt ="MULTIPOLYGON (((121.777612399000077 29.932831753000073,121.777085427000074 29.932277735000071,121.776493421000055 29.932637474000046,121.776365431000045 29.932715246000043,121.776343847000021 29.932728362000034,121.777183434000108 29.933262808000048,121.777217822 29.933227925000036,121.777230143000097 29.933215427000047,121.77725755499999 29.933187914000026,121.777612399000077 29.932831753000073)))";
//public static String testwkt="POLYGON((116.41465187072754 39.92266416549683,116.41533851623535 39.92026090621949,116.41302108764648 39.919660091400154,116.41091823577881 39.9209475517273,116.41130447387695 39.92292165756226,116.41465187072754 39.92266416549683))";
        public static String testwkt="MULTIPOLYGON (((118.549377093 28.8569337960001,118.53913082 28.8568231530001,118.536962593 28.8602215920001,118.535488519 28.864560753,118.537194312 28.8689348940001,118.539654988 28.8698948110001,118.547041024 28.872463349,118.556569076 28.8734989470001,118.562278785 28.8695157110001,118.566923783 28.8658318070001,118.565212521 28.8617693470001,118.562782275 28.858632416,118.556085815 28.8573167280001,118.549377093 28.8569337960001)))";
    /**相关测试   succeed
     * @throws Exception ***/
    public static void testDoQuery() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        HTablePool datatablePool = new HTablePool(10, conf, "LCRA2");
        HTablePool indexdatatablePool =  new HTablePool(10, conf, Utils.GetSpatialIndexTableNameByDataTableName("LCRA2"));
        SpatialQueryClass sqc = new SpatialQueryClass(datatablePool, indexdatatablePool);
        String[] wktStrs= new String[]{testwkt};
        String[] cats = new String[]{"0000"};
        Date d1 = new Date();
        SpatialQueryCondition sqcon = new SpatialQueryCondition(cats, wktStrs);

        List<Result> results = sqc.DoQuery(sqcon, QueryFilterManager.GetPositionListFilter());
        Date d2 = new Date();
        System.out.println(results.size());
        System.out.println(d1);
        System.out.println(d2);
    }

    /*
     * 测试空间统计  18813条记录，消时25s
     */
    public static void testDoStatistics() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        HTablePool datatablePool = new HTablePool(10, conf, "LCRA2");
        HTablePool indexdatatablePool =  new HTablePool(10, conf, Utils.GetSpatialIndexTableNameByDataTableName("LCRA2"));
        SpatialQueryClass sqc = new SpatialQueryClass(datatablePool, indexdatatablePool);
        String[] wktStrs= new String[]{testwkt};
        String[] cats = new String[]{"0110"};
        Date d1 = new Date();
        SpatialQueryCondition sqcon = new SpatialQueryCondition(cats, wktStrs);
        CategoryStatisticInfo statisticInfo = sqc.DoStatistics(sqcon, "");
        Date d2 = new Date();
        System.out.println(statisticInfo.SUMMARY);
        for(int i=0;i<statisticInfo.CATEGORYSTATISTICLIST.size();i++){
            System.out.println(statisticInfo.CATEGORYSTATISTICLIST.get(i));
        }
        System.out.println(d1);
        System.out.println(d2);
    }

    /*
     *1min 22s
     */
    public static void testBatchRpcCall() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        HTablePool datatablePool = new HTablePool(10, conf, "LCRA1");
        String[] wktStrs= new String[]{testwkt};
        String[] cats = new String[]{"00"};
        HTablePool indexdatatablePool =  new HTablePool(10, conf, Utils.GetSpatialIndexTableNameByDataTableName("LCRA1"));
        Date d1 = new Date();
        SpatialQueryCondition sqcon = new SpatialQueryCondition(cats, wktStrs);
        final SpatialStatisticRequest request = sqcon.GetSpatialStatisticRequest(indexdatatablePool);

        //	IRequestTaskSpliter<SpatialStatisticRequest> taskSpliter = new SpatialStatisticTaskSpliter(request,5);
        IRequestTaskSpliter<SpatialStatisticRequest> taskSpliter = new RegionBasedSpatialStatisticTaskSpliter(request);
        RpcCallClass<CategoryStatisticInfo, SpatialStatisticRequest> rpcCall = new SpatialStatisticRpcCallClass(datatablePool);
        BatchRpcServer<CategoryStatisticInfo, SpatialStatisticRequest> server = new BatchRpcServer<CategoryStatisticInfo, SpatialStatisticProtos.SpatialStatisticRequest>(taskSpliter, rpcCall);
        List<Map<byte[],CategoryStatisticInfo>> result = server.ParallelBatchRpc();
        int sum = 0;
        System.out.println("线程数："+result.size());
        for(int i=0;i<result.size();i++){
            int tn=0;
            for(Entry<byte[],CategoryStatisticInfo> entry:result.get(i).entrySet()){
                sum+=entry.getValue().SUMMARY.COUNT;
                System.out.println(entry.getValue().SUMMARY.COUNT);
                tn++;
            }
            System.out.println("tn:"+tn);
            tn = 0;
        }
        Date d2 = new Date();
        System.out.println(d1);
        System.out.println(d2);
        System.out.println(sum);


    }
}
