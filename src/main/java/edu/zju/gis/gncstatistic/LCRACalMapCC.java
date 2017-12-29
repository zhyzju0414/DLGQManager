package edu.zju.gis.gncstatistic;

import com.esri.core.geometry.*;
import com.esri.core.geometry.SpatialReference;
import edu.zju.gis.hbase.tool.MeasureClass;
import edu.zju.gis.spark.TransformationTool.Mdb2wkt;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.gdal.osr.*;
import scala.Tuple2;

import java.util.*;

/**
 * Created by hls on 2017/6/28.
 * 所获取的Grid下对应的地类图斑面积  RDD的key中含CC码和FID
 */
public class LCRACalMapCC implements PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>  {
    //cells:乡镇图斑
    List<Tuple2<String,Iterable<Tuple2<String, Geometry>>>> cells;
    int xzqCode;

    public LCRACalMapCC(List<Tuple2<String,Iterable<Tuple2<String, Geometry>>>> cells,int xzqCode){
        this.cells = cells;
        this.xzqCode = xzqCode;
    }

    @Override
    public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
        List<Tuple2<String,String>> out = new ArrayList<Tuple2<String, String>>();
        String gridCode = stringIterableTuple2._1().split("#")[0];
        if(gridCode.equals("ERROR")||gridCode.equals("skip")){
            return out.iterator();
        }
        Grid grid = Grid.Parse(gridCode);
        LatLonGridArchitecture countyGridArc = (LatLonGridArchitecture)GridArchitectureFactory.GetGridArchitecture(0.2D,"latlon");
        Envelope gridBoundary = countyGridArc.GetSpatialRange(grid);
//        System.out.println(gridBoundary.getXMin() + "," + gridBoundary.getYMin() + "," + gridBoundary.getXMax() + "," + gridBoundary.getYMax());
        QuadTree quadtree = new QuadTree(new Envelope2D(gridBoundary.getXMin(),gridBoundary.getYMin(),gridBoundary.getXMax(),gridBoundary.getYMax()),8);
//        QuadTree quadtree = new QuadTree(new Envelope2D(0,0,180,90),8);
        Iterator<String> iterator =  stringIterableTuple2._2().iterator();
        Map<Integer,String> quadtreeMap = new HashMap<Integer,String>();
        // Iterator<Tuple2<Geometry, String>> iterator = longIterableTuple2._2.iterator();
        // 构建精细格网内地表覆盖图斑的四叉树索引
        int i = 0;
        while (iterator.hasNext()){
            String items = iterator.next();
            String item = items.split("\t")[1];
            if((!item.startsWith("POLYGON"))&&(!item.startsWith("MULTIPOLYGON"))){
                System.out.println(" ===== THIS WKT IS WRONG IN LCRAMAP ======");
                System.out.println(item);
                continue;
            }
            Envelope2D envelope = new Envelope2D();
            Geometry itemGeo = GeometryEngine.geometryFromWkt(item, WktImportFlags.wktImportDefaults, Geometry.Type.Unknown);
            itemGeo.queryEnvelope2D(envelope);
            //System.out.println(envelope.getLowerLeft().getX() + "," + envelope.getLowerLeft().getY() + "," + envelope.getUpperRight().getX() + "," + envelope.getUpperRight().getY() );
            int r = quadtree.insert(i,envelope);
            quadtreeMap.put(i,items);
            i++;
        }

        String source = "GEOGCS[\"GCS_China_Geodetic_Coordinate_System_2000\",DATUM[\"D_China_2000\",SPHEROID[\"CGCS2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433],METADATA[\"China\",73.62,16.7,134.77,53.55,0.0,0.0174532925199433,0.0,1067],AUTHORITY[\"EPSG\",4490]]";
//        String target = "PROJCS[\"CGCS2000_3_Degree_GK_CM_117E\",GEOGCS[\"GCS_China_Geodetic_Coordinate_System_2000\",DATUM[\"D_China_2000\",SPHEROID[\"CGCS2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Gauss_Kruger\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",117.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0],AUTHORITY[\"EPSG\",4548]]";
        String target = Mdb2wkt.WGS84_proj;
        org.gdal.osr.SpatialReference sourceProj = new org.gdal.osr.SpatialReference();
        org.gdal.osr.SpatialReference targetProj = new org.gdal.osr.SpatialReference();
        java.util.Vector vector1 = new java.util.Vector();
        vector1.add(source);
        sourceProj.ImportFromESRI(vector1);
        java.util.Vector vector2 = new java.util.Vector();
        vector2.add(target);
        targetProj.ImportFromESRI(vector2);
        CoordinateTransformation coordinateTransformation = new CoordinateTransformation(sourceProj,targetProj);

        // 对精细格网内的乡镇图斑循环，与地表覆盖图斑相交计算
        //遍历每个精细格网
        for(Tuple2<String,Iterable<Tuple2<String,Geometry>>> county:cells){
            if(!county._1().equals(stringIterableTuple2._1().split("#")[0])){
                // 如果county的精细格网编码与当前计算的LCRA精细格网编码不一致，则继续循环
                continue;
            } else {
                // System.out.println(" ====== GRID CODE ：" + county._1());
                // 如果county的精细格网编码与当前计算的LCRA精细格网编码一致，则进行计算，计算结束后跳出循环
                Iterator<Tuple2<String,Geometry>> countyIterator =  county._2().iterator();

                //遍历该精细格网内所有的乡镇图斑 zhy
                while(countyIterator.hasNext()){
                    Tuple2<String,Geometry> countyTuple = countyIterator.next();

//                    System.out.println(countyTuple._1());

                    Geometry countyGeo = countyTuple._2();
                    //根据四叉树找到与该乡镇或者高程带相交的lcra图斑
                    QuadTree.QuadTreeIterator lcraIterator = quadtree.getIterator(countyGeo,0);
                    int num = lcraIterator.next();
//                    System.out.println(" ========== COUNTYGEO QUERY ===========");
//                    System.out.println(num);
                    //遍历每个lcra图斑 并计算相交面积  输出的key值为（暂时）以lcra的pac划分标准的regioncode(12位) + （高程）+ cc码
                    while(num>=0){
                        String[] propertys = quadtreeMap.get(quadtree.getElement(num)).split("\t");
                        if(propertys.length < 2){
                            System.out.println(" ============ PROPERTY IS BAD =============");
                            num = lcraIterator.next();
                            continue;
                        }
                        String wkt = propertys[1];
                        Geometry lcraGeo = GeometryEngine.geometryFromWkt(wkt,WktImportFlags.wktImportDefaults, Geometry.Type.Unknown);
                        // Geometry lcraGeo = quadtreeMap.get(num)._2();
                        // 计算重叠和重叠面积
                        Geometry intersects = GeometryEngine.intersect(lcraGeo,countyGeo,null);
                        String inter_wkt = GeometryEngine.geometryToWkt(intersects,WkbExportFlags.wkbExportDefaults);
                        org.gdal.ogr.Geometry inter_geo = org.gdal.ogr.Geometry.CreateFromWkt(inter_wkt);
                        inter_geo.Transform(coordinateTransformation);
//                         double area = MeasureClass.EllipsoidArea(GeometryEngine.geometryToWkt(intersects,WktExportFlags.wktExportDefaults));
                        // 把行政区编码和CC码一起放进Key中
//                        if (area > 0) {
                        if(!intersects.isEmpty()){
                            try {
                                double area = inter_geo.Area();
//                                double area = intersects.calculateArea2D(); // TODO 这个计算是不精确的
//                                System.out.println(area);
                                //计算乡镇时不用计算高程信息
//                                String key = countyTuple._1.substring(0,12) + "#" + propertys[0].substring(12,16);
                                //进行高程带计算式需要加上高程信息（12到16位）
//                                String key = propertys[0].substring(0,12)+countyTuple._1.substring(12,16)+ "#"+propertys[0].substring(12,16);
                                //进行格网计算时需要用格网的key值
                                String key = countyTuple._1.substring(0,12) + "#" + propertys[0].substring(12,16);
                                // String key = countyTuple._1();
                                String value = String.valueOf(area);
                                Tuple2<String,String> outTp = new Tuple2<String, String>(key,value);
                                out.add(outTp);
                            } catch(Exception e){
                                System.out.println(" ========= THIS PROPERTY IS BAD ========" );
                                if(propertys.length > 0){
                                    System.out.println(propertys[0]);
                                }
                                num = lcraIterator.next();
                                continue;
                            }
                        }
                        num = lcraIterator.next();
                    }
                }
                break;
            }
        }
        return out.iterator();
    }
}
