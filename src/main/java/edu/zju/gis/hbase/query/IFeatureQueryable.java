package edu.zju.gis.hbase.query;

import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;

import edu.zju.gis.hbase.entity.CategoryStatisticInfo;
import edu.zju.gis.hbase.query.querycondition.QueryCondition;

/*
 *
 */
public interface IFeatureQueryable {

    /*
     *
     */
    List<Result> DoQuery(QueryCondition qc, Filter columnFilter) throws Exception;

    /*
     *
     */
    CategoryStatisticInfo DoStatistics(QueryCondition qc, String statisticType) throws Exception;;

}
