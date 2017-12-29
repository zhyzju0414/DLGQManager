package edu.zju.gis.gncstatistic;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.WktImportFlags;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by hls on 2017/6/26.
 */
public class LCRACalMapNoIndex implements PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String> {

    List<Tuple2<String,Iterable<Tuple2<String, Geometry>>>> cells;
    int xzqCode;

    public LCRACalMapNoIndex(List<Tuple2<String,Iterable<Tuple2<String, Geometry>>>> cells, int xzqCode){
        this.cells = cells;
        this.xzqCode = xzqCode;
    }

    @Override
    public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {

        List<Tuple2<String,String>> out = new ArrayList<Tuple2<String, String>>();

        Iterator<String> iterator =  stringIterableTuple2._2().iterator();
        List<Tuple2<String,Geometry>> lcraGeo = new LinkedList<Tuple2<String,Geometry>>();

        while(iterator.hasNext()){
            String[] values = iterator.next().split("\t");
            //String key = edu.zju.gis.hbase.tool.Utils.GetRegionCode(values[0]) + edu.zju.gis.hbase.tool.Utils.GetCategoryCode(values[0]);
            //lcraGeo.add(new Tuple2<String, Geometry>(key,GeometryEngine.geometryFromWkt(values[1],WktImportFlags.wktImportDefaults, Geometry.Type.Polygon)));
            lcraGeo.add(new Tuple2<String, Geometry>(values[0], GeometryEngine.geometryFromWkt(values[1], WktImportFlags.wktImportDefaults, Geometry.Type.Polygon)));
        }

        for(Tuple2<String,Iterable<Tuple2<String,Geometry>>> county:cells){
            if(!county._1().equals(stringIterableTuple2._1())){
                // 如果county的精细格网编码与当前计算的LCRA精细格网编码不一致，则继续循环
                continue;
            } else {
                Iterator<Tuple2<String,Geometry>> countyIterator =  county._2().iterator();
                while(countyIterator.hasNext()){
                    Tuple2<String,Geometry> countyTuple = countyIterator.next();
                    for(int i=0;i<lcraGeo.size();i++){
                       Geometry intersectGeo = GeometryEngine.intersect(countyTuple._2,lcraGeo.get(i)._2,null);
                        if(!intersectGeo.isEmpty()){
                            // double area = MeasureClass.EllipsoidArea(GeometryEngine.geometryToWkt(intersectGeo,WktExportFlags.wktExportDefaults));
                            double area = intersectGeo.calculateArea2D(); //TODO 这个计算并不精确，是用的经纬度投影面积计算
                            Tuple2<String,String> outTp = new Tuple2<String, String>(countyTuple._1,Double.toString(area));
                            out.add(outTp);
                        }
                    }

//                    while (iterator.hasNext()){
//                        String item = iterator.next();
//                        if((!item.startsWith("POLYGON"))&&(!item.startsWith("MULTIPOLYGON"))){
//                            System.out.println(" ===== THIS WKT IS WRONG IN LCRAMAP ======");
//                            System.out.println(item);
//                            continue;
//                        }
//                        Geometry lcraGeo = GeometryEngine.geometryFromWkt(item, WktImportFlags.wktImportDefaults, Geometry.Type.Polygon);
//                        // 计算重叠和重叠面积
//                        Geometry intersects = GeometryEngine.intersect(lcraGeo,countyTuple._2(),null);
//                        //double area = intersects.calculateArea2D();
//                        double area = MeasureClass.EllipsoidArea(GeometryEngine.geometryToWkt(intersects,WktExportFlags.wktExportDefaults));
//                        if(area > 0){
//                            System.out.println(" ============ 有数据结果啦啦啦啦啦 ============");
//                            System.out.println(area);
//                            String key = countyTuple._1();
//                            String value = String.valueOf(area);
//                            Tuple2<String,String> outTp = new Tuple2<String, String>(key,value);
//                            out.add(outTp);
//                        }
//                    }
                }
            }
        }


        return out.iterator();
    }
}
