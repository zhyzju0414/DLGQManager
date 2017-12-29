package edu.zju.gis.hbase.coprocessor;

import edu.zju.gis.hbase.entity.CategoryStatisticInfo;
import edu.zju.gis.hbase.tool.HTablePool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;

import javax.crypto.MacSpi;
import java.io.IOException;
import java.util.Map;
import edu.zju.gis.hbase.coprocessor.ResponseInfo;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

/**
 * Created by dlgq on 2017/8/1.
 */
public class zhyTest {
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        HTablePool datatablePool = new HTablePool(10, conf, "splittest");
        Map<byte[],ResponseInfo> map = null;
        final MultiColumnSumProtocol.CountRequest request = MultiColumnSumProtocol.CountRequest.newBuilder().setColumns("c1;c2").build();
        try {
            System.out.println("==========start to do statistics============");
            map = datatablePool.GetTableInstance().coprocessorService(MultiColumnSumProtocol.RowCountService.class,null, null, new Batch.Call<MultiColumnSumProtocol.RowCountService, ResponseInfo>() {
                public ResponseInfo call(MultiColumnSumProtocol.RowCountService service)throws IOException{
                    BlockingRpcCallback<MultiColumnSumProtocol.CountResponse> rpcCallBack = new BlockingRpcCallback<MultiColumnSumProtocol.CountResponse>();

                    ServerRpcController controller = new ServerRpcController();
                    service.getCountAndSum(controller,request,rpcCallBack);
                    MultiColumnSumProtocol.CountResponse reponse = rpcCallBack.get();
                    ResponseInfo responseInfo = new ResponseInfo();
                    responseInfo.count1 = reponse.getCount1();
                    responseInfo.count2 = reponse.getCount2();
                    responseInfo.sum1 = reponse.getSum1();
                    responseInfo.sum2 = reponse.getSum2();
                    return responseInfo;
                }
            });
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        ResponseInfo result = new ResponseInfo();
        //统计每个region上的结果
        for(ResponseInfo ri: map.values()){
            result.count1+= ri.count1;
            result.count2 += ri.count2;
            result.sum1 += ri.sum1;
            result.sum2 += ri.sum2;
        }
        System.out.println("Product1 has "+ result.count1+" users, average online time is "+result.sum1/1000/result.count1+" minutes");
        System.out.println("Product2 has "+ result.count2+" users, average online time is "+result.sum2/1000/result.count2+" minutes");
    }
}
