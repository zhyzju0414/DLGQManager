package edu.zju.gis.spark.SurfaceAreaCalculation;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import edu.zju.gis.gncstatistic.*;
import edu.zju.gis.hbase.tool.SpatialIndexBuilder;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.gdal.osr.CoordinateTransformation;
import scala.Tuple2;

import java.util.*;

/**
 * Created by dlgq on 2017/8/14.
 */
public class SheetPartitionPairFlatMapFunction implements PairFlatMapFunction<String, String, Tuple2<Geometry, String>> {
    GridArchitectureInterface sheetArchitecture = null;
    final String source = "GEOGCS[\"GCS_China_Geodetic_Coordinate_System_2000\",DATUM[\"D_China_2000\",SPHEROID[\"CGCS2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433],METADATA[\"China\",73.62,16.7,134.77,53.55,0.0,0.0174532925199433,0.0,1067],AUTHORITY[\"EPSG\",4490]]";
    //    final String target =  "PROJCS[\"CGCS2000_3_Degree_GK_CM_117E\",GEOGCS[\"GCS_China_Geodetic_Coordinate_System_2000\",DATUM[\"D_China_2000\",SPHEROID[\"CGCS2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Gauss_Kruger\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",117.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0],AUTHORITY[\"EPSG\",4548]]";
//    final String target = "PROJCS[\"WGS_1984_Web_Mercator\",GEOGCS[\"GCS_WGS_1984_Major_Auxiliary_Sphere\",DATUM[\"D_WGS_1984_Major_Auxiliary_Sphere\",SPHEROID[\"WGS_1984_Major_Auxiliary_Sphere\",6378137.0,0.0]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Mercator\"],PARAMETER[\"false_easting\",0.0],PARAMETER[\"false_northing\",0.0],PARAMETER[\"central_meridian\",0.0],PARAMETER[\"standard_parallel_1\",0.0],UNIT[\"Meter\",1.0],AUTHORITY[\"EPSG\",3785]]";
    Map<Integer,String> targetProjMap = new HashMap<Integer,String>();
    int gridSize;

    //对于sheet  gridsize就是scale
    public SheetPartitionPairFlatMapFunction(int gridSize, String gridType) {
        sheetArchitecture = GridArchitectureFactory.GetGridArchitecture(gridSize, gridType);
        this.gridSize = gridSize;
        //不同的分幅对应不同的投影  一共有十个投影
        this.targetProjMap.put(75,edu.zju.gis.spark.Utils.targetProj75);
        this.targetProjMap.put(81,edu.zju.gis.spark.Utils.targetProj81);
        this.targetProjMap.put(87,edu.zju.gis.spark.Utils.targetProj87);
        this.targetProjMap.put(93,edu.zju.gis.spark.Utils.targetProj93);
        this.targetProjMap.put(99,edu.zju.gis.spark.Utils.targetProj99);
        this.targetProjMap.put(105,edu.zju.gis.spark.Utils.targetProj105);
        this.targetProjMap.put(111,edu.zju.gis.spark.Utils.targetProj111);
        this.targetProjMap.put(117,edu.zju.gis.spark.Utils.targetProj117);
        this.targetProjMap.put(123,edu.zju.gis.spark.Utils.targetProj123);
        this.targetProjMap.put(129,edu.zju.gis.spark.Utils.targetProj129);
        this.targetProjMap.put(135,edu.zju.gis.spark.Utils.targetProj135);
    }

    @Override
    public Iterator<Tuple2<String, Tuple2<com.esri.core.geometry.Geometry, String>>> call(String s) throws Exception {
        String[] strArr = s.split("\t");
        Geometry geo = null;
//        CoordinateTransformation ct = edu.zju.gis.gncstatistic.Utils.ProjTransform(source, target);
        List<Tuple2<String, Tuple2<Geometry, String>>> result = new ArrayList<Tuple2<String, Tuple2<Geometry, String>>>();
        try {
            geo = GeometryEngine.geometryFromWkt(strArr[strArr.length - 1], 0, Geometry.Type.Unknown);
        } catch (Exception e) {
            System.out.println("=========this wkt is not right==========");
            return result.iterator();
        }
        String property = ExtractPropertyStr(strArr);
        Envelope2D boundary = new Envelope2D();
        geo.queryEnvelope2D(boundary);
//        Tuple2<Sheet,Sheet> sheetRange = sheetArchitecture.GetCellRowRange(boundary);
        Iterable<Sheet> sheetRange = ((SheetGridArchitecture) sheetArchitecture).GetRangeSheets(boundary);
        for (Sheet sheet : sheetRange) {
            //根据分幅号计算这个分幅所在的带号对应的投影
            int projNum = getProjNum(sheet);
            String targetProj = targetProjMap.get(projNum);
            CoordinateTransformation ct = edu.zju.gis.gncstatistic.Utils.ProjTransform(source, targetProj);
            Geometry clipedGeo = GeometryEngine.intersect(geo, sheetArchitecture.GetSpatialRange(sheet), null);
            if (!clipedGeo.isEmpty()) {
                org.gdal.ogr.Geometry pGeo = org.gdal.ogr.Geometry.CreateFromWkt(GeometryEngine.geometryToWkt(clipedGeo, 0));
                pGeo.Transform(ct);
                Geometry clipedGeo2 = GeometryEngine.geometryFromWkt(pGeo.ExportToWkt(), 0, Geometry.Type.Unknown);
                double area = clipedGeo2.calculateArea2D();
                double length = clipedGeo2.calculateLength2D();
                //TODO 如果这个clipedGeo面积大于某个阈值  则将这个要素继续切碎
                if (area > 100000 ) {
                    List<Geometry> geoList = edu.zju.gis.spark.Utils.cutPolygon(clipedGeo2, 500);
                    for (Geometry g : geoList) {
                        area = g.calculateArea2D();
                        length = g.calculateLength2D();
                        String geoproperty = property + area + "\t" + length + "\t" + sheet.toString();
                        result.add(new Tuple2<String, Tuple2<Geometry, String>>(sheet.toString(), new Tuple2<Geometry, String>(g, geoproperty)));
                    }
                } else {
                    String geoproperty = property + area + "\t" + length + "\t" + sheet.toString();
                    result.add(new Tuple2<String, Tuple2<Geometry, String>>(sheet.toString(), new Tuple2<Geometry, String>(clipedGeo2, geoproperty)));
                }
            }
        }
        return result.iterator();
    }

    public static String ExtractPropertyStr(String[] strArr) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < strArr.length - 1; i++) {
            sb.append(strArr[i] + "\t");
        }
        return sb.toString();
    }

    public static Integer getProjNum(Sheet sheet){
        String sheetcode = sheet.toString();
        int colNum = Integer.valueOf(sheetcode.substring(1,3));
        int projNum = 180-(60-colNum)*6-3;
        return projNum;
    }
}
