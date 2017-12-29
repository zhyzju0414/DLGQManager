package edu.zju.gis.hbase.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import edu.zju.gis.hbase.tool.BatchGetServer.BatchGetCallable;

/*
 * 多线程执行Scan操作 zxw
 */
public class BatchScanServer<T> {

    private IScanTaskSpliter taskSpliter;   //任务分配接口
    private HTablePool htablePool;         //需要查询的表
    private IResultProcessor<T> resultProcessor;  //查询结果处理器


    public BatchScanServer(IScanTaskSpliter taskSpliter, HTablePool htablePool,
                           IResultProcessor<T> resultProcessor) {
        super();
        this.taskSpliter = taskSpliter;
        this.htablePool = htablePool;
        this.resultProcessor = resultProcessor;
    }

    /*
     * 处理单个任务的查询
     */
    public List<T> GetSingleTaskResult(Scan scan) throws IOException{
        ResultScanner rs = htablePool.GetTableInstance().getScanner(scan);
        List<T> resultlist = new ArrayList<T>();
//        System.out.println("resultlist.size: "+resultlist.size());
        for(Result result:rs){
            if(result!=null&&!result.isEmpty()){
                resultlist.add(resultProcessor.GetProcessedResult(result));
            }
        }
        return resultlist;
    }

    public List<T> ParallelBatchScan() throws IOException{
        List<T> resultlist = new ArrayList<T>();

        //任务划分
        List<Scan> scanTasks = taskSpliter.GetSplitTasks();
        if(scanTasks.size()<1){
            return null;
        }
        if(scanTasks.size()==1){
            //任务划分过小,以单线程执行
            return GetSingleTaskResult(scanTasks.get(0));
        }

        List<Future< List<T> >> futures = new ArrayList<Future< List<T>>>();

        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setNameFormat("ParallelBatchQuery");
        ThreadFactory factory = builder.build();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(scanTasks.size(), factory);
        //并发执行任务
        for(Scan scan : scanTasks){
            Callable< List<T>> callable = new BatchGetCallable(scan);
            FutureTask<List<T>> future = (FutureTask<List<T>>) executor.submit(callable);
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
                    resultlist.addAll((List<T>)f.get());
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
        return resultlist;
    }

    public class BatchGetCallable implements Callable{

        private Scan scan;

        public BatchGetCallable(Scan scan) {
            super();
            this.scan = scan;
        }

        @Override
        public List<T> call() throws Exception {
            // TODO Auto-generated method stub
            return  GetSingleTaskResult(scan);
        }

    }

}
