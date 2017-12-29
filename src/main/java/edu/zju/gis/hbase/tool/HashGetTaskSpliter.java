package edu.zju.gis.hbase.tool;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
/*
 * 基于Hash映射策略进行任务分配 zxw
 */
public class HashGetTaskSpliter implements IGetTaskSpliter {

    int parallel ; //并发线程数
    List<Get> totalGetList;  //总的Get数

    public HashGetTaskSpliter(List<Get> totalGetList,int parallel ) {
        super();
        this.parallel = parallel;
        this.totalGetList = totalGetList;
    }

    public HashGetTaskSpliter(List<Get> totalGetList) {
        super();
        this.parallel = 3;   //默认为3个线程
        this.totalGetList = totalGetList;
    }

    @Override
    public List<List<Get>> GetSplitTasks() {
        // TODO Auto-generated method stub
        List<List<Get>> gettasklist = null;
        if(totalGetList.size()<parallel){
            gettasklist = new ArrayList<List<Get>>(1);
            gettasklist.add(totalGetList);
        }else{
            gettasklist = new ArrayList<List<Get>>(this.parallel);
            for(int i = 0; i < parallel; i++  ){
                List<Get> lst = new ArrayList<Get>();
                gettasklist.add(lst);
            }

            for(int i = 0 ; i < totalGetList.size() ; i ++ ){
                gettasklist.get(i%parallel).add(totalGetList.get(i));
            }
        }
        return gettasklist;
    }

    public List<List<String>> SplitList(List<String> totalobjectList) {
        // TODO Auto-generated method stub
        List<List<String>> objectlist = null;
        if(totalGetList.size()<parallel){
            objectlist = new ArrayList<List<String>>(1);
            objectlist.add(totalobjectList);
        }else{
            objectlist = new ArrayList<List<String>>(parallel);
            for(int i = 0; i < parallel; i++  ){
                List<String> lst = new ArrayList<String>();
                objectlist.add(lst);
            }

            for(int i = 0 ; i < totalGetList.size() ; i ++ ){
                objectlist.get(i%parallel).add(totalobjectList.get(i));
            }
        }
        return objectlist;
    }



}
