package edu.zju.gis.spark.ParallelTools.RangeCondition;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.WktImportFlags;
import edu.zju.gis.gncstatistic.Grid;
import edu.zju.gis.gncstatistic.GridArchitectureFactory;
import edu.zju.gis.gncstatistic.LatLonGridArchitecture;
import edu.zju.gis.hbase.tool.ZCurve;
import edu.zju.gis.spark.SurfaceAreaCalculation.CalculationCondition.CalculationCondition;
import edu.zju.gis.spark.SurfaceAreaCalculation.CalculationCondition.PropertyCalculationCondition;
import edu.zju.gis.spark.SurfaceAreaCalculation.PropertyCalculationClass;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by Zhy on 2017/11/1.
 */
public class DistrictRange extends RangeCondition implements Serializable {
    //    public List<String> regionCode;
    public Map<String, Integer> regionCode;
    public int districtLevel;    //如果是省 level=1    市：level=2；   县：level=3

    public DistrictRange(String[] regionCode, List<String> wktList, int districtLevel) {
//        this.regionCode = new ArrayList<String>();
        this.regionCode = new HashMap<String, Integer>();
        for (String code : regionCode) {
            this.regionCode.put(code, 1);
        }
        this.wktList = wktList;
        this.districtLevel = districtLevel;
    }
}
