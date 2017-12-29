package edu.zju.gis.hbase.tool;

import org.apache.hadoop.hbase.client.Result;

/*
 * 对查询结果不做处理
 */
public class RawResultProcessor implements IResultProcessor<Result> {

    @Override
    public Result GetProcessedResult(Result res) {
        // TODO Auto-generated method stub
        return  res;
    }

}
