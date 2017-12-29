package edu.zju.gis.gncstatistic;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Frank on 2017/6/15.
 */
public class GridPartitionPairFlatMapFunction implements PairFlatMapFunction<String, Long, Tuple2<Geometry,String>> {


    GridArchitectureInterface gridArchitecture = null;

    public GridPartitionPairFlatMapFunction(double gridSize,String gridType){
        gridArchitecture = GridArchitectureFactory.GetGridArchitecture(gridSize,gridType);
    }


    @Override
    public Iterator<Tuple2<Long, Tuple2<Geometry, String>>> call(String s) throws Exception {

        String[] strArr = s.split("\t");
        Geometry geo = null;
        List<Tuple2<Long, Tuple2<Geometry, String>>> result = new ArrayList<Tuple2<Long, Tuple2<Geometry, String>>>();

        try {
            geo = GeometryEngine.geometryFromWkt(strArr[strArr.length-1],0, Geometry.Type.Polygon);
        } catch(Exception e){
            System.out.println(" ============= THIS WKT IS NOT RIGHT ============");
            System.out.println(strArr);
            return result.iterator();
        }

        String property = ExtractPropertyStr(strArr);
        Envelope2D boundary = new Envelope2D();
        geo.queryEnvelope2D(boundary);
        Tuple2<Grid,Grid> gridRange = gridArchitecture.GetCellRowRange(boundary);

        for(long i=gridRange._1().Row;i<=gridRange._2().Row;i++){
            for(long j=gridRange._1().Col;j<=gridRange._2.Col;j++){
                Grid grid = new Grid(i,j);
                Geometry clipedGeo = GeometryEngine.intersect(geo,gridArchitecture.GetSpatialRange(grid),null);
                if(!clipedGeo.isEmpty()){
                    double area = clipedGeo.calculateArea2D();
                    double length = clipedGeo.calculateLength2D();
                    String geoproperty = property+area+"\t"+length;
                    result.add(new Tuple2<Long, Tuple2<Geometry, String>>(Long.parseLong(grid.toString()),new Tuple2<Geometry, String>(clipedGeo,geoproperty)));
                }
            }

        }

        return result.iterator();
    }

    public static String ExtractPropertyStr(String[] strArr){
        StringBuffer sb = new StringBuffer();
        for(int i=0;i<strArr.length-1;i++){
            sb.append(strArr[i]+"\t");
        }
        return sb.toString();
    }
}
