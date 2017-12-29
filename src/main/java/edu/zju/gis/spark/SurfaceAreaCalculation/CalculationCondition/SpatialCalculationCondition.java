package edu.zju.gis.spark.SurfaceAreaCalculation.CalculationCondition;

/**
 * 根据要素的几何值进行筛选，计算面积
 * Created by Zhy on 2017/9/21.
 */
public class SpatialCalculationCondition extends CalculationCondition {
    public SpatialCalculationCondition(String[] categoryCode,String wkt){
        super(categoryCode,wkt);
    }
}
