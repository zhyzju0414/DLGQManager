package edu.zju.gis.hbase.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * 多线程执行Get操作 zxw
 */
public class BatchGetServer<T> {
    private IGetTaskSpliter taskSpliter;   //任务分配接口
    private HTablePool htablePool;         //需要查询的表
    private IResultProcessor<T> resultProcessor;  //查询结果处理器


    public BatchGetServer(IGetTaskSpliter taskSpliter, HTablePool htablePool,
                          IResultProcessor<T> resultProcessor) {
        super();
        this.taskSpliter = taskSpliter;
        this.htablePool = htablePool;
        this.resultProcessor = resultProcessor;
    }

    /*
     * 处理单个任务的查询
     */
    public List<T> GetSingleTaskResult(List<Get> getlist) throws IOException{
//        Result[] resultArr = new Result[getlist.size()];
      Result[] resultArr = htablePool.GetTableInstance().get(getlist);
//        for(int i=0;i<getlist.size();i++){
////            Result res =htablePool.GetTableInstance().get(getlist.get(i));
//            Filter filter = getlist.get(i).getFilter();
//            Get get = new Get(getlist.get(i).getRow());
//            get.setFilter(filter);
//            Result res =htablePool.GetTableInstance().get(get);
////            System.out.println("res:"+res.toString());
//            resultArr[i] = res;
//        }
        for(Result res :resultArr){
//            System.out.println("result: "+ res.toString());
//            System.out.println("siza = "+res.listCells().size());
        }
        List<T> resultlist = new ArrayList<T>();
        if(resultProcessor==null){
            for(int i=0;i<resultArr.length;i++){
                resultlist.add((T) resultArr[i]);
//                System.out.println("resultProcessor==null");
            }
        }else{
            for(int i=0;i<resultArr.length;i++){
                if(resultArr[i]!=null&&!resultArr[i].isEmpty()){
                    resultlist.add(resultProcessor.GetProcessedResult(resultArr[i]));
                    System.out.println("size = "+resultArr[i].listCells().size());
                }
            }
        }
//每个result里面还有多个cell
//        System.out.println("resultList: ");
//        for(T t:resultlist){
//            System.out.println("result=: "+ t.toString());
//        }
        return resultlist;
    }

    public List<T> ParallelBatchGet() throws IOException{
        List<T> resultlist = new ArrayList<T>();

        //任务划分
        List<List<Get>> getTasks = taskSpliter.GetSplitTasks();
        System.out.println("getTASK :"+ getTasks.size());
        if(getTasks.size()<1){
            return null;
        }
        if(getTasks.size()==1){
            //任务划分过小,以单线程执行
            System.out.println("====1");
            return GetSingleTaskResult(getTasks.get(0));
        }


        List<Future< List<T> >> futures = new ArrayList<Future< List<T>>>(5);

        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setNameFormat("ParallelBatchQuery");
        ThreadFactory factory = builder.build();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(getTasks.size(), factory);
        //并发执行任务
        for(List<Get> gets : getTasks){
            System.out.println("lcra: get.size="+gets.size());
            Callable< List<T>> callable = new BatchGetCallable(gets);
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

        private List<Get> getlist;

        public BatchGetCallable(List<Get> getlist) {
            super();
            this.getlist = getlist;
        }

        @Override
        public List<T> call() throws Exception {
            // TODO Auto-generated method stub
            return  GetSingleTaskResult(getlist);
        }

    }
}
