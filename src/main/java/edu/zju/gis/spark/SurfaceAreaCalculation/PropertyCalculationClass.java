package edu.zju.gis.spark.SurfaceAreaCalculation;

import edu.zju.gis.spark.SurfaceAreaCalculation.CalculationCondition.CalculationCondition;
import edu.zju.gis.spark.SurfaceAreaCalculation.CalculationCondition.PropertyCalculationCondition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Zhy on 2017/9/21.
 */
public class PropertyCalculationClass implements IFeatureCalcalation,Serializable {

    public JavaPairRDD<String, Iterable<String>> getFilteredRDD(CalculationCondition condition, List<String> sheetCodes, String lcraFilePath, JavaSparkContext sc) throws Exception {
        JavaPairRDD<String, Iterable<String>> allLcraByCodeRDD = null;
        if (condition instanceof PropertyCalculationCondition) {
            String[] categoryCode = condition.getCategoryCode();
            String regionCode = ((PropertyCalculationCondition) condition).getRegionCode();

            String lcraPath = lcraFilePath + "/" + sheetCodes.get(0).substring(0, 3) + "/" + sheetCodes.get(0) + "/wkt";
            JavaPairRDD<String, String> allLcraRDD = sc.textFile(lcraPath).filter(new CcRegionFilter(categoryCode, regionCode)).mapToPair(new PairFunction<String, String, String>() {
                @Override
                public Tuple2<String, String> call(String s) throws Exception {
                    String[] values = s.split("\t");
                    String code = values[0];
                    Tuple2<String, String> res = new Tuple2<String, String>(code, s);
                    return res;
                }
            });
            for (int i = 1; i < sheetCodes.size(); i++) {
                //读取这个code下面的所有lcra要素
                lcraPath = lcraFilePath + "/" + sheetCodes.get(i).substring(0, 3) + "/" + sheetCodes.get(i) + "/wkt";
                JavaPairRDD<String, String> lcraRDD = sc.textFile(lcraPath).filter(new CcRegionFilter(categoryCode, regionCode)).mapToPair(new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] values = s.split("\t");
                        String code = values[0];
                        Tuple2<String, String> res = new Tuple2<String, String>(code, s);
                        return res;
                    }
                });
                //用union将所有的RDD合并成一个RDD  假设叫temp1
                allLcraRDD = allLcraRDD.union(lcraRDD);
            }
            allLcraByCodeRDD = allLcraRDD.groupByKey(sheetCodes.size());
            return allLcraByCodeRDD;
        }
        throw new Exception("CalculationCondition type error,should be PropertyCalculationCondition");
    }

    static class CcRegionFilter implements Function<String, Boolean> {
        //        String[] categoryCode;
        Map<String, Integer> categoryCodeMap;
        String regionCode;

        CcRegionFilter(String[] categoryCode, String regionCode) {
            categoryCodeMap = new HashMap<String,Integer>();
            if (categoryCode != null) {
                for (String cc : categoryCode) {
                    this.categoryCodeMap.put(cc, 1);
                }
            }
            this.regionCode = regionCode;
        }

        @Override
        public Boolean call(String s) throws Exception {
            String rowkey = s.split("\t")[1];
            String regionCode = rowkey.substring(0, 6);
            String CC = rowkey.substring(12, 16);
            if(categoryCodeMap==null&&regionCode==null)   //没有过滤条件
                return true;
            if(categoryCodeMap==null){
                //计算所有类别的要素
                if(regionCode.equals(this.regionCode))
                    return true;
                else
                    return false;
            }
            if(regionCode==null){
                if(this.categoryCodeMap.containsKey(CC))
                    return true;
                else
                    return false;
            }
            if (regionCode.equals(this.regionCode) && this.categoryCodeMap.containsKey(CC)) {
                return true;
            } else {
                return false;
            }
        }

    }
}
