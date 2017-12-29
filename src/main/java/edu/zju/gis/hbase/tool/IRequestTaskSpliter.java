package edu.zju.gis.hbase.tool;

import java.util.List;


public interface IRequestTaskSpliter<V> {

    public List<V> GetSplitTasks();

}
