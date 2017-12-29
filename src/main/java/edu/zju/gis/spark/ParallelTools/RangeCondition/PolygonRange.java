package edu.zju.gis.spark.ParallelTools.RangeCondition;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.WktImportFlags;
import edu.zju.gis.gncstatistic.Grid;
import edu.zju.gis.gncstatistic.GridArchitectureFactory;
import edu.zju.gis.gncstatistic.LatLonGridArchitecture;
import edu.zju.gis.hbase.tool.ZCurve;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import sun.security.util.PolicyUtil;

import java.util.*;

/**
 * Created by Zhy on 2017/11/1.
 */
public class PolygonRange extends RangeCondition {

//    public PolygonRange(String[] wkt) {
//        this.wkt = new ArrayList<String>();
//        for (String str : wkt) {
//            this.wkt.add(str);
//        }
//    }

    public PolygonRange(List<String> wktList){
        this.wktList = wktList;
    }

}
