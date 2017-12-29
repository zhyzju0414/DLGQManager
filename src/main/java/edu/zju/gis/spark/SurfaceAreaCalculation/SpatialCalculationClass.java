package edu.zju.gis.spark.SurfaceAreaCalculation;

import edu.zju.gis.spark.SurfaceAreaCalculation.CalculationCondition.CalculationCondition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * Created by Zhy on 2017/9/21.
 */
public class SpatialCalculationClass implements IFeatureCalcalation {
    @Override
    public JavaPairRDD<String, Iterable<String>> getFilteredRDD(CalculationCondition condition, List<String> sheetCode, String lcraFilePath, JavaSparkContext sc) throws Exception {
        return null;
    }
}
