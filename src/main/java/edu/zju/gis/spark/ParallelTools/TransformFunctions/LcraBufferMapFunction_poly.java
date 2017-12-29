package edu.zju.gis.spark.ParallelTools.TransformFunctions;

import com.esri.core.geometry.*;
import edu.zju.gis.gncstatistic.Grid;
import edu.zju.gis.gncstatistic.GridArchitectureFactory;
import edu.zju.gis.gncstatistic.LatLonGridArchitecture;
import edu.zju.gis.spark.TransformationTool.Mdb2wkt;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.gdal.osr.CoordinateTransformation;
import scala.Tuple2;

import java.util.*;

import static edu.zju.gis.gncstatistic.Utils.ProjTransform;

/**
 * Created by Zhy on 2017/11/5.
 */
public class LcraBufferMapFunction_poly implements FlatMapFunction<Tuple2<String, Iterable<String>>, String> {
    List<Tuple2<String, Iterable<Tuple2<String, Geometry>>>> gridInSingleJob;  //格网号  属性  geometry
    double bufferDist;
    static {

    }

    public LcraBufferMapFunction_poly(List<Tuple2<String, Iterable<Tuple2<String, Geometry>>>> gridInSingleJob, double bufferDist) {
        this.gridInSingleJob = gridInSingleJob;
        this.bufferDist = bufferDist;
    }

    @Override
    public Iterator<String> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
//        List<Tuple2<String, String>> out = new ArrayList<Tuple2<String, String>>();
        String source = "GEOGCS[\"GCS_China_Geodetic_Coordinate_System_2000\",DATUM[\"D_China_2000\",SPHEROID[\"CGCS2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433],METADATA[\"China\",73.62,16.7,134.77,53.55,0.0,0.0174532925199433,0.0,1067],AUTHORITY[\"EPSG\",4490]]";
        String target = "PROJCS[\"CGCS2000_3_Degree_GK_CM_117E\",GEOGCS[\"GCS_China_Geodetic_Coordinate_System_2000\",DATUM[\"D_China_2000\",SPHEROID[\"CGCS2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Gauss_Kruger\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",117.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0],AUTHORITY[\"EPSG\",4548]]";
//

        List<String> out = new ArrayList<String>();
        String gridCode = stringIterableTuple2._1().split("#")[0];
        if (gridCode.equals("ERROR") || gridCode.equals("skip")) {
            return out.iterator();
        }
        Grid grid = Grid.Parse(gridCode);
        System.out.println("gridcode:"+gridCode);
        System.out.println(grid.Col+" "+grid.Row);
        LatLonGridArchitecture countyGridArc = (LatLonGridArchitecture) GridArchitectureFactory.GetGridArchitecture(0.2D, "latlon");
        Envelope gridBoundary = countyGridArc.GetSpatialRange(grid);
        QuadTree quadtree = new QuadTree(new Envelope2D(gridBoundary.getXMin(), gridBoundary.getYMin(), gridBoundary.getXMax(), gridBoundary.getYMax()), 8);
        Iterator<String> iterator = stringIterableTuple2._2().iterator();
        Map<Integer, String> quadtreeMap = new HashMap<Integer, String>();
        int i = 0;
        //遍历该格网中每一个lcra要素  并插入到四叉树中
        while (iterator.hasNext()) {
            String items = iterator.next();
            String item = items.split("\t")[1];
            if ((!item.startsWith("POLYGON")) && (!item.startsWith("MULTIPOLYGON"))) {
                System.out.println(" ===== THIS WKT IS WRONG IN LCRAMAP ======");
                System.out.println(item);
                continue;
            }
            Envelope2D envelope = new Envelope2D();
            Geometry itemGeo = GeometryEngine.geometryFromWkt(item, WktImportFlags.wktImportDefaults, Geometry.Type.Unknown);
            itemGeo.queryEnvelope2D(envelope);
            quadtreeMap.put(i, items);
            quadtree.insert(i,envelope);
            i++;
            System.out.println(i);
        }
        System.out.println("quadtreeElementCount"+quadtree.getElementCount());
        //遍历每一个格网
        for (Tuple2<String, Iterable<Tuple2<String, Geometry>>> county : gridInSingleJob) {
            System.out.println("======开始循环======");
            System.out.println(county._1());
            if (!county._1().equals(stringIterableTuple2._1().split("#")[0])) {
                //如果格网号不相同 直接跳过
                continue;
            } else {
                System.out.println("开始进行叠加");
                Iterator<Tuple2<String, Geometry>> countyIterator = county._2().iterator();
                while (countyIterator.hasNext()) {
                    Tuple2<String, Geometry> countyTuple = countyIterator.next();
                    Geometry countyGeo = countyTuple._2();
                    QuadTree.QuadTreeIterator lcraIterator = quadtree.getIterator(countyGeo, 0);
                    int num = lcraIterator.next();
                    System.out.println("相交的lcra个数"+num);
                    while (num >= 0) {
                        String[] propertys = quadtreeMap.get(quadtree.getElement(num)).split("\t");
                        String wkt = propertys[1];
                        String ccCode =propertys[0].substring(12,16);
                        String regionCode = propertys[0].substring(0,6);
//                        System.out.println(wkt);
                        Geometry lcraGeo = GeometryEngine.geometryFromWkt(wkt, WktImportFlags.wktImportDefaults, Geometry.Type.Unknown);
                        // Geometry lcraGeo = quadtreeMap.get(num)._2();
                        // 计算重叠和重叠面积
                        Geometry intersects = GeometryEngine.intersect(lcraGeo, countyGeo, null);
                        if(intersects.isEmpty()){
                            num = lcraIterator.next();
                            continue;
                        }
                        Polygon bufferPolygon = GeometryEngine.buffer(intersects,SpatialReference.create(4326),bufferDist);
                        String bufferWkt = GeometryEngine.geometryToWkt(bufferPolygon,WktExportFlags.wktExportDefaults);
                        org.gdal.ogr.Geometry buffer_geo = org.gdal.ogr.Geometry.CreateFromWkt(bufferWkt);
                        String outputWkt = regionCode+"\t"+ccCode+"\t"+bufferWkt;
                        out.add(outputWkt);
                        num = lcraIterator.next();
                    }
                }
                break;
            }
        }
        return out.iterator();
    }


}
