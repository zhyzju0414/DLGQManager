package edu.zju.gis.spark.ParallelTools.TransformFunctions;

import com.esri.core.geometry.*;
import edu.zju.gis.spark.ParallelTools.RangeCondition.DistrictRange;
import edu.zju.gis.spark.ParallelTools.RangeCondition.RangeCondition;
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
public class LcraBufferMapFunction_Dist implements FlatMapFunction<Tuple2<String, Iterable<String>>, String> {
    Map<String,Integer> countyFilter;
    int districtLevel;
    double bufferDist;

    public LcraBufferMapFunction_Dist(DistrictRange districtRange, double bufferDist){
        this.countyFilter = districtRange.regionCode;
        this.districtLevel = districtRange.districtLevel;
        this.bufferDist =  bufferDist;
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
        while(iterator.hasNext()){
            String region_wkt = iterator.next();
            String[] values = region_wkt.split("\t");
            String regionCode = values[0].substring(0,6);
            String wkt = values[1];
//            System.out.println(Utils.rightPad(regionCode.substring(0,districtLevel*2),(6-districtLevel*2)));
            if(countyFilter.containsKey(Utils.rightPad(regionCode.substring(0,districtLevel*2),(6-districtLevel*2)))){
                System.out.println("包含regioncode:" + regionCode);
                Geometry lcraGeo = GeometryEngine.geometryFromWkt(wkt, WktImportFlags.wktImportDefaults, Geometry.Type.Unknown);
                Polygon bufferPolygon = GeometryEngine.buffer(lcraGeo, SpatialReference.create(4490),bufferDist);
                String bufferWkt = GeometryEngine.geometryToWkt(bufferPolygon,WktExportFlags.wktExportDefaults);
                String outputWkt = regionCode+"\t"+ccCode+"\t"+bufferWkt;
                out.add(outputWkt);
            }else{
                continue;
            };
        }
        return out.iterator();
    }
}
