package edu.zju.gis.hbase.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ServiceException;

import edu.zju.gis.hbase.query.RpcCallClass;
import edu.zju.gis.hbase.tool.BatchGetServer.BatchGetCallable;




/*
 * 多线程执行远程调用操作 zxw
 */
public class BatchRpcServer<T,R> {

    private IRequestTaskSpliter<R> taskSpliter;
    private RpcCallClass<T,R> rpcCallObj;



    public BatchRpcServer(IRequestTaskSpliter<R> taskSpliter, RpcCallClass<T, R> rpcCallObj) {
        super();

        this.taskSpliter = taskSpliter;
        this.rpcCallObj = rpcCallObj;
    }

    public List<Map<byte[],T>> ParallelBatchRpc() throws Exception{

        List<Map<byte[],T>> result = new ArrayList<Map<byte[],T>>();
        //任务划分
        List<R> rpcTasks = taskSpliter.GetSplitTasks();
        System.out.println("rpcTasks.size= "+rpcTasks.size());
        if(rpcTasks.size()<1){
            return null;
        }
        if(rpcTasks.size()==1){
            //任务划分过小,以单线程执行
            rpcCallObj.initializRequest(rpcTasks.get(0));
            result.add(rpcCallObj.call());
            return result;
        }

        List<Future< Map<byte[],T> >> futures = new ArrayList<Future< Map<byte[],T>>>(5);

        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setNameFormat("ParallelBatchRpc");
        ThreadFactory factory = builder.build();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(rpcTasks.size(), factory);
        //并发执行任务
        for(R request : rpcTasks){
            //初始化请求
            RpcCallClass<T,R> cloneObj = (RpcCallClass<T,R>)rpcCallObj.clone();
            cloneObj.initializRequest(request);
            Callable< Map<byte[],T>> callable = cloneObj;
            FutureTask< Map<byte[],T>> future = (FutureTask< Map<byte[],T>>) executor.submit(callable);
            futures.add(future);
        }
        executor.shutdown();

        // 等待查询线程执行完成
        try {
            boolean stillRunning = !executor.awaitTermination(
                    5000000, TimeUnit.MILLISECONDS);
            if (stillRunning) {
                try {
                    executor.shutdownNow();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        } catch (InterruptedException e) {
            try {
                Thread.currentThread().interrupt();
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }

        // 收集查询结果
        for (Future f : futures) {
            try {
                if(f.get() != null)
                {
                    result.add(( Map<byte[],T>)f.get());
                }
            } catch (InterruptedException e) {
                try {
                    Thread.currentThread().interrupt();
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

}
