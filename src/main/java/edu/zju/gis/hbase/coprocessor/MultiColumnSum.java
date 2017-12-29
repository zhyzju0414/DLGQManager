package edu.zju.gis.hbase.coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dlgq on 2017/8/1.
 */
public class MultiColumnSum extends MultiColumnSumProtocol.RowCountService implements Coprocessor,CoprocessorService {
    private RegionCoprocessorEnvironment env;
    public Service getService(){
        return this;
    }
    //这个方法在coprocessor启动时调用  这里判断了是否在一个region内被使用  而不是master
    public void start(CoprocessorEnvironment env)throws IOException{
        if(env instanceof RegionCoprocessorEnvironment){
            this.env =(RegionCoprocessorEnvironment) env;
        }else{
            throw new CoprocessorException("Must be loaded on table region!");
        }
    }

    //这个方法在coprocessor结束的时候被调用 等于关闭资源等 这里为空
    public void stop(CoprocessorEnvironment env)throws IOException{
    }
    //整个类的核心方法，用于实现真正的业务逻辑
    //应用场景：有一个columnfamily f1 有两个column  分别是c1和c2  两个列分别是两款产品的同时在线时间
    //现在要计算：在线时长超过10分钟的是真实用户 要计算这两个产品的真实用户有多少
    //还要计算：这两个产品的真实用户的平均在线时长为多少？
    //协处理器的处理逻辑：
    //请求是两个列名  如果多一个产品的话  可以直接使用
    //计算两个产品的真实用户  count1和 count2
    //计算两个产品的真实在线总时长为sum1 和 sum2
    //将每个region的sum和count相加，计算两个产品的平均值

    public void getCountAndSum(RpcController controller, MultiColumnSumProtocol.CountRequest request, RpcCallback<MultiColumnSumProtocol.CountResponse> done) {
        //分别为第一个产品的真实用户数量、第二个产品的真实用户数量、第一个产品的总在线时长、第二个产品的总在线时长
        long[] values = { 0, 0, 0, 0 };
        //请求为列名
        String columns = request.getColumns();  //required string columns = 1;
        if (columns == null || "".equals(columns))
            throw new NullPointerException("you need specify the columns");
        //获得所有的列名
        String[] columnArray = columns.split(";");
        System.out.println("==========="+columnArray.length+ " products in total");
        //根据request创建一个scanner 然后用它创建一个InternalScanner  可以进行更高效的scan
        Scan scan = new Scan();
        //将需要扫描的列加入scan
        for (String column : columnArray) {
            scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column));
        }
        //request对应一个response
        MultiColumnSumProtocol.CountResponse response = null;
        InternalScanner scanner = null;
        try {
            scanner = env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<Cell>();
            boolean hasMore = false;
            //对扫描出来的每一行进行分析处理
            do {
                System.out.println("正在扫描１行");
                hasMore = scanner.next(results);
                if (results.size() < 2)
                    continue;
                Cell kv0 = results.get(0);
                //value1是第一个产品的在线时长
                long value1 = Long.parseLong(Bytes.toString(CellUtil.cloneValue(kv0)));
                Cell kv1 = results.get(1);
                //value２是第二个产品的在线时长
                long value2 = Long.parseLong(Bytes.toString(CellUtil.cloneValue(kv1)));
                if(value1 > 60000){
                    values[0] += 1;
                    values[2] += value1;
                }
                if(value2 > 60000){
                    values[1] += 1;
                    values[3] += value2;
                }
                results.clear();
            } while (hasMore);
            //现在获得了所有产品的真实用户个数以及所有用户的总在线时长
            // 生成response
            //调用response的各个set()方法，设置返回的结果
            //不太理解
            response = MultiColumnSumProtocol.CountResponse.newBuilder().setCount1(values[0]).setCount2(values[1]).setSum1(values[2]).setSum2(values[3]).build();

        } catch (IOException e) {
            e.printStackTrace();
            ResponseConverter.setControllerException(controller, e);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {
                }
            }
        }
        //将结果返回到客户端
        done.run(response);
    }
}
