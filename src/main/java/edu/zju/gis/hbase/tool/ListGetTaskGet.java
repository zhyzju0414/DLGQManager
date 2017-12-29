package edu.zju.gis.hbase.tool;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;

public class ListGetTaskGet implements IGetTaskSpliter {

    List<List<Get>> totalGetList;  //总的Get数

    public ListGetTaskGet(List<List<Get>> totalGetList) {
        super();
        this.totalGetList = totalGetList;
    }


    @Override
    public List<List<Get>> GetSplitTasks() {
        // TODO Auto-generated method stub
        List<List<Get>> splitedTask = new ArrayList<List<Get>>();
        for(int i=0;i<totalGetList.size();i++){
            HashGetTaskSpliter hashSplitTask = new HashGetTaskSpliter(totalGetList.get(i));
            splitedTask.addAll(hashSplitTask.GetSplitTasks());
        }
        return splitedTask;
    }

}
