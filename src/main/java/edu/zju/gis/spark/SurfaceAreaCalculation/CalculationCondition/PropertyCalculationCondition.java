package edu.zju.gis.spark.SurfaceAreaCalculation.CalculationCondition;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.GeometryEngine;
import edu.zju.gis.hbase.tool.RegionManager;

import java.util.Arrays;

/**
 * 根据要素的属性进行筛选 计算面积
 * Created by Zhy on 2017/9/21.
 */
public class PropertyCalculationCondition extends CalculationCondition{
    public String regionCode;
//    public Envelope envelope;

    public PropertyCalculationCondition(String[] categoryCode,String wkt,String regionCode){
        super(categoryCode,wkt);
        if(regionCode!=null){
            this.regionCode = RegionManager.GetStandardRegionCode(regionCode);
        }
    }

    public String toString() {
        return "PropertyCalculationCondition [regionCode=" + regionCode
                + ", getCategoryCode()=" + Arrays.toString(getCategoryCode())
                + "]";
    }
    public String getRegionCode() {
        return regionCode;
    }

}
