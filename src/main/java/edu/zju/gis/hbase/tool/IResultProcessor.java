/**
 * 单个Result处理器
 */
package edu.zju.gis.hbase.tool;

import org.apache.hadoop.hbase.client.Result;

/**
 * @author zxw
 *
 */
public interface IResultProcessor<T> {
    //处理Result对象
    public T GetProcessedResult(Result res);


}
