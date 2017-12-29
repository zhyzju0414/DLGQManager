package edu.zju.gis.hbase.query;

import java.util.Map;

import edu.zju.gis.hbase.entity.CategoryStatisticInfo;
import edu.zju.gis.hbase.entity.FeatureClassObj;
import edu.zju.gis.hbase.entity.FeatureClassObjList;
import edu.zju.gis.hbase.entity.PositionList;


public interface IFeatureQueryService {

    public FeatureClassObj GetFeatureInfo(String rowcode);


    public PositionList GetFeatureList(Map<String, String> queryParameter);


    public FeatureClassObjList GetFeatureListByPage(Map<String, String> queryParameter);


    public CategoryStatisticInfo GetStatisticInfo(Map<String, String> queryParameter);


    public FeatureClassObj GetPointQueryFeature(Map<String, String> queryParameter);

}
