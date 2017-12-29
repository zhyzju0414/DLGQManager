package edu.zju.gis.hbase.query;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;

import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticRequest;
import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticResponse;
import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticService;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticResponse;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticService;
import edu.zju.gis.hbase.entity.CategoryStatisticInfo;
import edu.zju.gis.hbase.entity.StatisticInfo;
import edu.zju.gis.hbase.tool.HTablePool;
import edu.zju.gis.hbase.tool.RegionManager;

public class PropertyStatisticRpcCallClass extends RpcCallClass<CategoryStatisticInfo, CategoryStatisticRequest>{

    private HTablePool htablePool;         //需要查询的表
    CategoryStatisticRequest request;


    public PropertyStatisticRpcCallClass(HTablePool htablePool) {
        super();
        this.htablePool = htablePool;
    }


    @Override
    public void initializRequest(CategoryStatisticRequest request) {
        // TODO Auto-generated method stub
        this.request = request;
    }


    @Override
    public Map<byte[],CategoryStatisticInfo> call() throws Exception {
        // TODO Auto-generated method stub
        byte[] startkey = null;
        byte[] stopkey =  null;

        if(!request.getRegioncode().equals(RegionManager.provinceCode)){
            String[] regionRange = RegionManager.GetRowkeyRange(request.getRegioncode());
            startkey = Bytes.toBytes(regionRange[0]);
            stopkey =  Bytes.toBytes(regionRange[1]);
        }

        try {
            Map<byte[], CategoryStatisticInfo> results = htablePool.GetTableInstance().coprocessorService(CategoryStatisticService.class, startkey, stopkey, new Batch.Call<CategoryStatisticService,CategoryStatisticInfo>() {
                public CategoryStatisticInfo call(CategoryStatisticService counter) throws IOException {
                    ServerRpcController controller = new ServerRpcController();
                    BlockingRpcCallback<CategoryStatisticResponse> rpcCallback =
                            new BlockingRpcCallback<CategoryStatisticResponse>();
                    counter.getStatisticInfo(controller, request, rpcCallback);

                    CategoryStatisticResponse response = rpcCallback.get();
                    List<CategoryStatisticResponse.CategoryStatisticInfo> lst = response.getCategoryStatisticInfoList();

                    Map<String,StatisticInfo> CATEGORYSTATISTICINFO = new HashMap<String,StatisticInfo>();
                    StatisticInfo SUMMARY = new StatisticInfo();

                    for(int i=0;i<lst.size();i++){
                        SUMMARY.COUNT+=lst.get(i).getCount();
                        SUMMARY.AREA+=lst.get(i).getArea();
                        SUMMARY.LENGTH+=lst.get(i).getLength();
                        CATEGORYSTATISTICINFO.put(lst.get(i).getCategory(), new StatisticInfo(lst.get(i).getCount(),lst.get(i).getArea(),lst.get(i).getLength()));
                    }
                    CategoryStatisticInfo statisticInfo = new CategoryStatisticInfo(SUMMARY,CATEGORYSTATISTICINFO);
                    return statisticInfo;

                }
            });
            return results;
        } catch (Throwable e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public Object clone() throws CloneNotSupportedException {
        // TODO Auto-generated method stub
        PropertyStatisticRpcCallClass cloneObj = null;
        cloneObj = (PropertyStatisticRpcCallClass)super.clone();
        return cloneObj;
    }

}
