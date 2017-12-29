package edu.zju.gis.hbase.query;


import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import scala.Tuple2;
import edu.zju.gis.hbase.entity.PositionList;
import edu.zju.gis.hbase.query.querycondition.QueryCondition;

/*
 * 用于对分页查询结果进行缓存,缓存置换策略采用最长时间未访问替换 zxw
 */
public class CacheManager {

    private Map<Integer,PositionList> cacheMap;
    private int maxSize = 10000000;  //最大缓存10000000条查询记录
    private int currentSize;
    private Queue<Tuple2<Date,Integer>> timeQueue;  //记录每一个查询结果的缓存时间

    public CacheManager() {
        cacheMap = new HashMap<Integer,PositionList>();
        timeQueue = new PriorityQueue<Tuple2<Date,Integer>>(20000,new Comparator<Tuple2<Date,Integer>>() {
            @Override
            public int compare(Tuple2<Date, Integer> o1,
                               Tuple2<Date, Integer> o2) {
                // TODO Auto-generated method stub
                return o1._1.compareTo(o2._1);
            }
        });
    }

    public CacheManager(int cacheSize) {
        cacheMap = new HashMap<Integer,PositionList>();
        timeQueue = new PriorityQueue<Tuple2<Date,Integer>>(20000,new Comparator<Tuple2<Date,Integer>>() {
            @Override
            public int compare(Tuple2<Date, Integer> o1,
                               Tuple2<Date, Integer> o2) {
                // TODO Auto-generated method stub
                return o1._1.compareTo(o2._1);
            }
        });
        maxSize = cacheSize;
    }



    public void CacheResult(QueryCondition qc,PositionList result) throws Exception{
        if(RemoveExpiredData(result.getPositionList().size())){
            int hashcode =GetQueryParameterHashCode(qc);
            cacheMap.put(hashcode, result);
            timeQueue.add(new Tuple2<Date,Integer>(new Date(),hashcode));
            currentSize+=result.getPositionList().size();
        }
    }

    /*
     * 根据需要缓存的数据大小，移除最久未访问的数据,返回true表示缓存空间足够容纳size
     */
    private boolean RemoveExpiredData(int size) throws Exception{
        if(maxSize>=size){
            while(size+currentSize>maxSize){
                Tuple2<Date,Integer> expiredDataId = timeQueue.poll();
                if(expiredDataId!=null){
                    PositionList expiredData = cacheMap.remove(expiredDataId._2);
                    currentSize = currentSize - expiredData.getPositionList().size();
                }
            }
            return true;
        }else{
            return false;
        }
    }

    public PositionList GetData(QueryCondition qc,int startIndex,int length){

        if(startIndex<1){
            startIndex = 1;
        }
        int hashcode =GetQueryParameterHashCode(qc);
        PositionList data = cacheMap.get(hashcode);
        if(data!=null){
            System.out.println("cache hit");
            //更新访问时间
            UpdatetimeQueue(hashcode);
            PositionList targetData = new PositionList();
            int actuallength = data.getPositionList().size()>startIndex+length-1?startIndex+length-1:data.getPositionList().size();
            for(int i=startIndex-1;i<actuallength;i++){
                targetData.AddPosition(data.getPositionList().get(i));
            }
            return targetData;
        }
        return null;
    }

    public PositionList GetData(QueryCondition qc){

        int hashcode =GetQueryParameterHashCode(qc);
        PositionList data = cacheMap.get(hashcode);
        if(data!=null){
            System.out.println("cache hit");
            //更新访问时间
            UpdatetimeQueue(hashcode);
            return data;
        }
        return null;
    }

    private int GetQueryParameterHashCode(QueryCondition qc){

        return qc.hashCode();
    }

    private void UpdatetimeQueue(int key){
        Iterator<Tuple2<Date,Integer>> iterator = timeQueue.iterator();
        while(iterator.hasNext()){
            Tuple2<Date,Integer> item = iterator.next();
            if(item._2==key){
                timeQueue.remove(item);
                timeQueue.add(new Tuple2<Date,Integer>(new Date(),key));
                return;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Queue<Tuple2<Date,Integer>> timeQueue = new PriorityQueue<Tuple2<Date,Integer>>(20000,new Comparator<Tuple2<Date,Integer>>() {

            @Override
            public int compare(Tuple2<Date, Integer> o1,
                               Tuple2<Date, Integer> o2) {
                // TODO Auto-generated method stub
                return o1._1.compareTo(o2._1);
            }
        });
        timeQueue.add(new Tuple2<Date, Integer>(new Date(),1));
        Thread.sleep(1000);
        timeQueue.add(new Tuple2<Date, Integer>(new Date(),2));
        Thread.sleep(1000);
        timeQueue.add(new Tuple2<Date, Integer>(new Date(),3));
        Thread.sleep(1000);
        timeQueue.add(new Tuple2<Date, Integer>(new Date(),4));
        Thread.sleep(1000);

        Iterator<Tuple2<Date,Integer>> iterator = timeQueue.iterator();
        while(iterator.hasNext()){
            Tuple2<Date,Integer> item = iterator.next();
            if(item._2==2){
                timeQueue.remove(item);
                timeQueue.add(new Tuple2<Date,Integer>(new Date(),2));
                break;
            }
        }

        System.out.println(timeQueue.poll().toString());
        System.out.println(timeQueue.poll().toString());
        System.out.println(timeQueue.poll().toString());
        System.out.println(timeQueue.poll().toString());
    }
}
