package edu.zju.gis.spark.ParallelTools.TransformFunctions;

import com.esri.core.geometry.*;
import edu.zju.gis.spark.ParallelTools.RangeCondition.DistrictRange;
import edu.zju.gis.spark.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Zhy on 2017/11/8.
 */
public class LcraClipMapFunction_Dist implements FlatMapFunction<Tuple2<String, Iterable<String>>, String> {
    Map<String,Integer> countyFilter;
    int districtLevel;
    String calType;

    public LcraClipMapFunction_Dist(DistrictRange districtRange,String calType){
        this.countyFilter = districtRange.regionCode;
        this.districtLevel = districtRange.districtLevel;
        this.calType = calType;
    }

    @Override
    public Iterator<String> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
        List<String> out = new ArrayList<String>();
        String gridCode = stringIterableTuple2._1().split("#")[0];
        if (gridCode.equals("ERROR") || gridCode.equals("skip")) {
            return out.iterator();
        }
        Iterator<String> iterator = stringIterableTuple2._2().iterator();
        String ccCode = stringIterableTuple2._1().split("#")[1];
        String gridOrSheetCode = stringIterableTuple2._1().split("#")[0];
        while(iterator.hasNext()){
            String region_wkt = iterator.next();
            String[] values = region_wkt.split("\t");
            String rowkey = values[0];
            String regionCode = values[0].substring(0,6);
            String wkt = values[1];
            if(countyFilter.containsKey(Utils.rightPad(regionCode.substring(0,districtLevel*2),(6-districtLevel*2)))){
                String outputWkt = null;
                if(calType.equals("surfaceArea")){
                    outputWkt = gridOrSheetCode+"\t"+rowkey+"\t"+wkt;
                }else{
                    outputWkt = regionCode+"\t"+ccCode+"\t"+wkt;
                }
                out.add(outputWkt);
            }else{
                continue;
            };
        }
        return out.iterator();
    }
}
