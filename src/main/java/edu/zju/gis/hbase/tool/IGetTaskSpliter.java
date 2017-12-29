package edu.zju.gis.hbase.tool;

import java.util.List;

import org.apache.hadoop.hbase.client.Get;

/*
 * 批量Get操作的任务分配接口
 */
public interface IGetTaskSpliter {

    public List<List<Get>> GetSplitTasks();
}
