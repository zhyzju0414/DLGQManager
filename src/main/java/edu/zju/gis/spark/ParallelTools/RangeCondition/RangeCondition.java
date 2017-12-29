package edu.zju.gis.spark.ParallelTools.RangeCondition;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.WktImportFlags;
import edu.zju.gis.gncstatistic.*;
import edu.zju.gis.hbase.tool.ZCurve;
import edu.zju.gis.spark.SurfaceAreaCalculation.CalculationCondition.CalculationCondition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Zhy on 2017/11/1.
 * 选择的计算区域的形式  省市县、自定义区域
 */
public abstract class RangeCondition implements Serializable{
    public static List<String> wktList;
//    public static JavaRDD<String> wktRDD;

    public static JavaPairRDD<String, Iterable<Tuple2<String, Geometry>>> convertRange2Grid(JavaSparkContext sc){
//        System.out.println("wkt = "+wkt);
        JavaRDD<String> regionWktRDD = sc.parallelize(wktList);
        final Broadcast<Integer> xzqCodeBC = sc.broadcast(0);
        JavaPairRDD<String, Iterable<Tuple2<String, Geometry>>> gridRDD = regionWktRDD.flatMapToPair(new PairFlatMapFunction<String, String, Tuple2<String, Geometry>>() {
            @Override
            public Iterator<Tuple2<String, Tuple2<String, Geometry>>> call(String s) throws Exception {
                String wkt = null;
                String regionID = null;
                List<Tuple2<String, Tuple2<String, Geometry>>> out = new ArrayList<Tuple2<String, Tuple2<String, Geometry>>>();
                try {
                    String[] propertys = s.split("\t");
//                    regionID = propertys[10];
                    regionID = propertys[0];
//                    System.out.println(regionID.substring(0,2));
                    System.out.println(regionID);
                    wkt = propertys[propertys.length - 1];
                    if ((!wkt.startsWith("POLYGON")) && (!wkt.startsWith("MULTIPOLYGON"))) {
                        System.out.println(" ========= THIS WKT IS NOT RIGHT ========== ");
                        System.out.println(wkt);
                        System.out.println(s);
                        return out.iterator();
                    }
                    LatLonGridArchitecture countyGridArc = (LatLonGridArchitecture) GridArchitectureFactory.GetGridArchitecture(0.2D, "latlon");
                    Geometry countyGeo = GeometryEngine.geometryFromWkt(wkt, WktImportFlags.wktImportDefaults, Geometry.Type.Polygon);
//                    System.out.println(" ========= COUNTY GEO AREA ========== ");
//                    System.out.println(MeasureClass.EllipsoidArea(wkt));
                    Envelope2D countyEnv = new Envelope2D();
                    countyGeo.queryEnvelope2D(countyEnv);
                    Tuple2<Grid, Grid> gridRange = countyGridArc.GetCellRowRange(countyEnv);
                    long startRow = gridRange._1().Row;
                    long endRow = gridRange._2().Row;
                    long startCol = gridRange._1().Col;
                    long endCol = gridRange._2().Col;
                    for (long i = startRow; i <= endRow; i++) {
                        for (long j = startCol; j <= endCol; j++) {
                            Geometry inGrid = GeometryEngine.intersect(countyGridArc.GetSpatialRange(new Grid(i, j)), countyGeo, null);
                            if (inGrid == null || inGrid.isEmpty()) {
                                continue;
                            }
                            StringBuffer outProperty = new StringBuffer();
                            outProperty.append(propertys[xzqCodeBC.getValue()]);
                            //outProperty.append(propertys[propertys.length-1]);
                            Tuple2<String, Tuple2<String, Geometry>> outCell = new Tuple2<String, Tuple2<String, Geometry>>(Long.toString(ZCurve.GetZValue(0, i, j)), new Tuple2<String, Geometry>(outProperty.toString(), inGrid));
                            out.add(outCell);
                        }
                    }

                    return out.iterator();
                } catch (Exception e) {
                    e.printStackTrace();
                    return out.iterator();
                }
            }
        }).groupByKey();
        return gridRDD;
    }

    public List<String> getSheetRangeByWkt(){
        //先根据geoWkt生成MBR  选出分幅
        List<java.lang.String> sheetCode=new ArrayList<java.lang.String>();
        GridArchitectureInterface sheetArchitecture = GridArchitectureFactory.GetGridArchitecture(50000, "sheet");
        for(String geoWkt:wktList){
            Geometry geo = GeometryEngine.geometryFromWkt(geoWkt, 0, Geometry.Type.Unknown);
            Envelope2D boundary = new Envelope2D();
            geo.queryEnvelope2D(boundary);
//        System.out.println(boundary.xmin+" "+boundary.ymin+" "+boundary.xmax+" "+boundary.ymax );
            Iterable<Sheet> sheetRange = ((SheetGridArchitecture) sheetArchitecture).GetRangeSheets(boundary);
            for(Sheet sheet:sheetRange){
                sheetCode.add(sheet.toString());
            }
        }
        return sheetCode;
    }

    public List<String> getSheetRange(String geoWkt){
        //先根据geoWkt生成MBR  选出分幅
        List<java.lang.String> sheetCode=new ArrayList<java.lang.String>();
        GridArchitectureInterface sheetArchitecture = GridArchitectureFactory.GetGridArchitecture(50000, "sheet");
            Geometry geo = GeometryEngine.geometryFromWkt(geoWkt, 0, Geometry.Type.Unknown);
            Envelope2D boundary = new Envelope2D();
            geo.queryEnvelope2D(boundary);
//        System.out.println(boundary.xmin+" "+boundary.ymin+" "+boundary.xmax+" "+boundary.ymax );
            Iterable<Sheet> sheetRange = ((SheetGridArchitecture) sheetArchitecture).GetRangeSheets(boundary);
            for(Sheet sheet:sheetRange){
                sheetCode.add(sheet.toString());
            }
        return sheetCode;
    }


    public JavaPairRDD<String, Iterable<Tuple2<String, Geometry>>> convertRange2Sheet(JavaSparkContext sc){
        JavaRDD<String> regionWktRDD = sc.parallelize(wktList);
        final Broadcast<Integer> xzqCodeBC = sc.broadcast(0);
        JavaPairRDD<String, Iterable<Tuple2<String, Geometry>>>  sheetRDD = regionWktRDD.flatMapToPair(new PairFlatMapFunction<String, String, Tuple2<String, Geometry>>()  {
            @Override
            public Iterator<Tuple2<String, Tuple2<String, Geometry>>> call(String s) throws Exception {
                String wkt = s;
                List<Tuple2<String, Tuple2<String, Geometry>>> out = new ArrayList<Tuple2<String, Tuple2<String, Geometry>>>();
                try{
                if ((!wkt.startsWith("POLYGON")) && (!wkt.startsWith("MULTIPOLYGON"))) {
                    System.out.println(" ========= THIS WKT IS NOT RIGHT ========== ");
                    System.out.println(wkt);
                    System.out.println(s);
                    return out.iterator();
                }
                SheetGridArchitecture sheetArc = (SheetGridArchitecture) GridArchitectureFactory.GetGridArchitecture(50000, "sheet");
                Geometry countyGeo = GeometryEngine.geometryFromWkt(wkt, WktImportFlags.wktImportDefaults, Geometry.Type.Polygon);
                List<String> sheetRange = getSheetRange(wkt);
                for(String sheetCode:sheetRange){
                    Geometry inGrid = GeometryEngine.intersect(sheetArc.GetSpatialRange(new Sheet(sheetCode)), countyGeo, null);
                    if (inGrid == null || inGrid.isEmpty()) {
                        continue;
                    }
                    StringBuffer outProperty = new StringBuffer();
                    outProperty.append("NoProperty");
                    //outProperty.append(propertys[propertys.length-1]);
                    Tuple2<String, Tuple2<String, Geometry>> outCell = new Tuple2<String, Tuple2<String, Geometry>>(sheetCode, new Tuple2<String, Geometry>(outProperty.toString(), inGrid));
                    out.add(outCell);
                }
                return out.iterator();
                }catch (Exception e){
                    e.printStackTrace();
                    return out.iterator();
                }
            }
        }).groupByKey();
        return sheetRDD;
    }
}
