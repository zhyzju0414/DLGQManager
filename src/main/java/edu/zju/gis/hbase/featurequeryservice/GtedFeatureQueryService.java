package edu.zju.gis.hbase.featurequeryservice;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;

import edu.zju.gis.hbase.entity.FeatureClassObj;
import edu.zju.gis.hbase.query.FeatureQueryStatisticService;

public class GtedFeatureQueryService {

    //地表覆盖数据查询服务
    private FeatureQueryStatisticService gtedservice;

    public GtedFeatureQueryService(String gtedTableName) {
        super();
        gtedservice = new FeatureQueryStatisticService(gtedTableName);
    }

    public FeatureClassObj GetPointQueryFeature(Map<String, String> queryParameter) {
        // TODO Auto-generated method stub
        return gtedservice.GetPointQueryFeature(queryParameter);
    }


}
