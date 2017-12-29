package edu.zju.gis.spark.SurfaceAreaCalculation.CalculationCondition;

import edu.zju.gis.hbase.tool.CategoryManager;

/**
 * 面积计算条件类
 * Created by Zhy on 2017/9/21.
 */
public abstract class CalculationCondition {
    public static String[] categoryCode;
    public static String wkt;

    public CalculationCondition(String[] ccFilter,String wkt){
        super();
        if(ccFilter!=null){
            this.categoryCode = ccFilter;
            for(int i=0;i<this.categoryCode.length;i++){
                this.categoryCode[i] = CategoryManager.getStandCategoryCode(this.categoryCode[i]);
            }
        }
        this.wkt = wkt;
    }
    public String[] getCategoryCode() {
        return categoryCode;
    }
    public String getWkt(){
        return wkt;
    }

}
