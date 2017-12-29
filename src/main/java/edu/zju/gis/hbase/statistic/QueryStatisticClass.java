package edu.zju.gis.hbase.statistic;

import java.util.List;

import org.apache.hadoop.hbase.client.Result;

import edu.zju.gis.hbase.entity.CategoryStatisticInfo;

public abstract class QueryStatisticClass {

    private String statisticName;  //统计分析名称

    public abstract CategoryStatisticInfo DoStatistic(List<Result> results,String[] categoryCodeArr);


}
