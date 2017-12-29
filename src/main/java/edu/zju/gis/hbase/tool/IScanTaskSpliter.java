/**
 *
 */
package edu.zju.gis.hbase.tool;

import java.util.List;

import org.apache.hadoop.hbase.client.Scan;

/**
 * @author zxw
 *
 */
public interface IScanTaskSpliter {

    public List<Scan> GetSplitTasks();
}
