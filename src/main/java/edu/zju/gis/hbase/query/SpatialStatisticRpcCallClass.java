package edu.zju.gis.hbase.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;

import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticRequest;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticResponse;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticService;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticRequest.GetInfo;
import edu.zju.gis.hbase.entity.CategoryStatisticInfo;
import edu.zju.gis.hbase.entity.StatisticInfo;
import edu.zju.gis.hbase.tool.HTablePool;

public class SpatialStatisticRpcCallClass extends RpcCallClass<CategoryStatisticInfo, SpatialStatisticRequest> {

    private HTablePool htablePool;         //需要查询的表
    SpatialStatisticRequest request;



    public SpatialStatisticRpcCallClass(HTablePool htablePool) {
        super();
        this.htablePool = htablePool;

    }

    public  void initializRequest(SpatialStatisticRequest request){
        this.request = request;
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

    @Override
    public Map<byte[],CategoryStatisticInfo> call() throws Exception {
        // TODO Auto-generated method stub
        byte[][] rowkeyRange = GetRowKeyRange(request);
        try {
            Map<byte[],CategoryStatisticInfo> results = htablePool.GetTableInstance().coprocessorService(SpatialStatisticService.class, rowkeyRange[0], rowkeyRange[1], new Batch.Call<SpatialStatisticService,CategoryStatisticInfo>(){

                @Override
                public CategoryStatisticInfo call(SpatialStatisticService counter)
                        throws IOException {
                    // TODO Auto-generated method stub
                    System.out.println("=========start scan on regionserver==========");
                    ServerRpcController controller = new ServerRpcController();
                    BlockingRpcCallback<SpatialStatisticResponse> rpcCallback =
                            new BlockingRpcCallback<SpatialStatisticResponse>();
                    counter.getStatisticInfo(controller, request, rpcCallback);

                    //获取远程调用结果
                    SpatialStatisticResponse response = rpcCallback.get();
                    List<SpatialStatisticResponse.SpatialStatisticInfo> lst = response.getSpatialStatisticInfoList();

                    Map<String,StatisticInfo> CATEGORYSTATISTICINFO = new HashMap<String,StatisticInfo>();
                    StatisticInfo SUMMARY = new StatisticInfo();

                    for(int i=0;i<lst.size();i++){
                        System.out.println("第"+i+"个region下的个数为："+lst.get(i).getCount());
                        System.out.println("第"+i+"个region下的面积为："+lst.get(i).getArea());

                        if(lst.get(i).getCount()!=0){
                            SUMMARY.AREA+=lst.get(i).getArea();
                            SUMMARY.LENGTH+=lst.get(i).getLength();
                            CATEGORYSTATISTICINFO.put(lst.get(i).getCategory(), new StatisticInfo(lst.get(i).getCount(),lst.get(i).getArea(),lst.get(i).getLength()));
                        }else {
                            CATEGORYSTATISTICINFO.put(lst.get(i).getCategory(), new StatisticInfo(lst.get(i).getCount(),0,0));
                        }
                        SUMMARY.COUNT+=lst.get(i).getCount();
                    }
                    CategoryStatisticInfo statisticInfo = new CategoryStatisticInfo(SUMMARY,CATEGORYSTATISTICINFO);
                    return statisticInfo;
                }

            });
            return results;
        }catch (Throwable e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public Object clone() throws CloneNotSupportedException {
        // TODO Auto-generated method stub
        SpatialStatisticRpcCallClass cloneObj = null;
        cloneObj = (SpatialStatisticRpcCallClass)super.clone();
        return cloneObj;
    }




}
