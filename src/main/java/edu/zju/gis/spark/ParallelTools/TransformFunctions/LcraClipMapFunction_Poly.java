package edu.zju.gis.spark.ParallelTools.TransformFunctions;

import com.esri.core.geometry.*;
import com.esri.core.geometry.Geometry;
import edu.zju.gis.gncstatistic.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.gdal.ogr.*;
import org.gdal.osr.CoordinateTransformation;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Zhy on 2017/11/5.
 */
public class LcraClipMapFunction_Poly implements FlatMapFunction<Tuple2<String, Iterable<String>>, String> {
    List<Tuple2<String, Iterable<Tuple2<String, Geometry>>>> gridInSingleJob;  //格网号  属性  geometry
    String calType;

    public LcraClipMapFunction_Poly(List<Tuple2<String, Iterable<Tuple2<String, Geometry>>>> gridInSingleJob,String calType){
        this.gridInSingleJob = gridInSingleJob;
        this.calType = calType;
    }

    @Override
    public Iterator<String> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
        List<String> out = new ArrayList<String>();
        String gridCode = stringIterableTuple2._1().split("#")[0];
        if (gridCode.equals("ERROR") || gridCode.equals("skip")) {
            return out.iterator();
        }
        Envelope envelope_grid;
        QuadTree quadtree;
        if(calType.equals("surfaceArea")){
            Sheet sheet = new Sheet(gridCode);
            System.out.println("sheetcode:"+gridCode);
            SheetGridArchitecture sheetGridArchitecture = (SheetGridArchitecture)GridArchitectureFactory.GetGridArchitecture(50000,"sheet");
            envelope_grid = sheetGridArchitecture.GetSpatialRange(sheet);
        }
        else{
            Grid grid = Grid.Parse(gridCode);
            System.out.println("gridcode:"+gridCode);
            System.out.println(grid.Col+" "+grid.Row);
            LatLonGridArchitecture countyGridArc = (LatLonGridArchitecture) GridArchitectureFactory.GetGridArchitecture(0.2D, "latlon");
            envelope_grid = countyGridArc.GetSpatialRange(grid);
//            Envelope gridBoundary = countyGridArc.GetSpatialRange(grid);
        }
        quadtree = new QuadTree(new Envelope2D(envelope_grid.getXMin(), envelope_grid.getYMin(), envelope_grid.getXMax(), envelope_grid.getYMax()), 8);
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
            if(calType.equals("surfaceArea")){
                //如果是地表面积计算  把lcra要素转成经纬度坐标
                org.gdal.ogr.Geometry geo = org.gdal.ogr.Geometry.CreateFromWkt(item);
                String target = "GEOGCS[\"GCS_China_Geodetic_Coordinate_System_2000\",DATUM[\"D_China_2000\",SPHEROID[\"CGCS2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433],METADATA[\"China\",73.62,16.7,134.77,53.55,0.0,0.0174532925199433,0.0,1067],AUTHORITY[\"EPSG\",4490]]";
                String source = "PROJCS[\"WGS_1984_Web_Mercator_Auxiliary_Sphere\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Mercator_Auxiliary_Sphere\"],PARAMETER[\"False_Easting\",0.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",0.0],PARAMETER[\"Standard_Parallel_1\",0.0],PARAMETER[\"Auxiliary_Sphere_Type\",0.0],UNIT[\"Meter\",1.0],AUTHORITY[\"EPSG\",3857]]";
                CoordinateTransformation coordinateTransformation = Utils.ProjTransform(source,target);
                //geo是经纬度的坐标
                geo.Transform(coordinateTransformation);
                itemGeo = GeometryEngine.geometryFromWkt(geo.ExportToWkt(), WktImportFlags.wktImportDefaults, Geometry.Type.Unknown);
                items = items.split("\t")[0]+"\t"+geo.ExportToWkt();
            }
            itemGeo.queryEnvelope2D(envelope);
            System.out.println(envelope.xmin+" "+envelope.ymin+" "+envelope.xmax+" "+envelope.ymax);
            quadtreeMap.put(i, items);
            System.out.println(quadtree.insert(i,envelope));
//            quadtree.insert(i,envelope);
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
                    int count = 0;
                    while (num >= 0) {
                        String[] propertys = quadtreeMap.get(quadtree.getElement(num)).split("\t");
                        //如果是
                        String wkt = propertys[1];
                        String rowkey = propertys[0];
                        String regioncode = propertys[0].substring(0,6);
                        String ccCode =propertys[0].substring(12,16);
//                        System.out.println(wkt);
                        Geometry lcraGeo = GeometryEngine.geometryFromWkt(wkt, WktImportFlags.wktImportDefaults, Geometry.Type.Unknown);
                        // Geometry lcraGeo = quadtreeMap.get(num)._2();
                        // 计算重叠和重叠面积
                        Geometry intersects = GeometryEngine.intersect(lcraGeo, countyGeo, null);
                        if(calType.equals("surfaceArea")){
                            //如果是地表面积计算  要再把经纬度坐标转成投影坐标
                            String source = "GEOGCS[\"GCS_China_Geodetic_Coordinate_System_2000\",DATUM[\"D_China_2000\",SPHEROID[\"CGCS2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433],METADATA[\"China\",73.62,16.7,134.77,53.55,0.0,0.0174532925199433,0.0,1067],AUTHORITY[\"EPSG\",4490]]";
                            String target = "PROJCS[\"WGS_1984_Web_Mercator_Auxiliary_Sphere\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Mercator_Auxiliary_Sphere\"],PARAMETER[\"False_Easting\",0.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",0.0],PARAMETER[\"Standard_Parallel_1\",0.0],PARAMETER[\"Auxiliary_Sphere_Type\",0.0],UNIT[\"Meter\",1.0],AUTHORITY[\"EPSG\",3857]]";
                            CoordinateTransformation coordinateTransformation = Utils.ProjTransform(source,target);
                            org.gdal.ogr.Geometry intersect = org.gdal.ogr.Geometry.CreateFromWkt( GeometryEngine.geometryToWkt(intersects,WktExportFlags.wktExportDefaults));
                            intersect.Transform(coordinateTransformation);
                            intersects = GeometryEngine.geometryFromWkt(intersect.ExportToWkt(), WktImportFlags.wktImportDefaults, Geometry.Type.Unknown);
                        }
                        if(intersects.isEmpty()){
                            num = lcraIterator.next();
                            continue;
                        }else {
                            System.out.println("有相交");
                        }
                        String intersectWkt = GeometryEngine.geometryToWkt(intersects,WktExportFlags.wktExportDefaults);
                        String outputWkt=null;
                        if(calType.equals("surfaceArea")){
                            String sheetcode =stringIterableTuple2._1().split("#")[0];
                            outputWkt =sheetcode +"\t"+rowkey+"\t"+intersectWkt;
                        }else{
                            outputWkt =regioncode+"\t"+ccCode+"\t"+intersectWkt;
                        }
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
