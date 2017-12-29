package edu.zju.gis.spark.SurfaceAreaCalculation.CalculationCondition;

import java.util.Map;

/**
 * Created by Zhy on 2017/9/21.
 */
public class CalculationConditionFactory {

    //参数名称
    public static String geoWKTParameterName = "GEOWKT";
    public static String categoryCodeParameterName = "CATEGORYCODE";
    public static String regionCodeParameterName = "REGIONCODE";

    public static CalculationCondition createCalculationCondition(Map<String,String> parameterMap){
        String[] categoryCode= null;
        if(parameterMap.containsKey(categoryCodeParameterName)){
            categoryCode = parameterMap.get(categoryCodeParameterName).split(",");
        }
        if(parameterMap.containsKey(regionCodeParameterName)){
            //根据要素属性进行筛选  不需要进行空间运算
            String regionCode = parameterMap.get(regionCodeParameterName);
            String geoWkt = parameterMap.get(geoWKTParameterName);
            return new PropertyCalculationCondition(categoryCode,geoWkt,regionCode);
        }else{
            //要进行空间运算
            String geoWkt = parameterMap.get(geoWKTParameterName);
            return new SpatialCalculationCondition(categoryCode,geoWkt);
        }
    }
}
