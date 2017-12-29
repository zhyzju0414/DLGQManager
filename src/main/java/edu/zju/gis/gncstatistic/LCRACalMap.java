package edu.zju.gis.gncstatistic;

import com.esri.core.geometry.*;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by hls on 2017/6/25.
 */
public class LCRACalMap implements PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>{

    List<Tuple2<String,Iterable<Tuple2<String, Geometry>>>> cells;
    int xzqCode;

    public LCRACalMap(List<Tuple2<String,Iterable<Tuple2<String, Geometry>>>> cells, int xzqCode){
        this.cells = cells;
        this.xzqCode = xzqCode;
    }

    @Override
    public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
        List<Tuple2<String,String>> out = new ArrayList<Tuple2<String, String>>();
        Grid grid = Grid.Parse(stringIterableTuple2._1());
        LatLonGridArchitecture countyGridArc = (LatLonGridArchitecture) GridArchitectureFactory.GetGridArchitecture(0.2D,"latlon");
        Envelope gridBoundary = countyGridArc.GetSpatialRange(grid);
        QuadTree quadtree = new QuadTree(new Envelope2D(gridBoundary.getXMin(),gridBoundary.getYMin(),gridBoundary.getXMax(),gridBoundary.getYMax()),8);
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
            //Tuple2<String,Geometry> item = iterator.next();
            //Geometry itemGeo = item._2();
            Envelope2D envelope = new Envelope2D();
            GeometryEngine.geometryFromWkt(item, WktImportFlags.wktImportDefaults, Geometry.Type.Polygon).queryEnvelope2D(envelope);
            quadtree.insert(i,envelope);
            quadtreeMap.put(i,items);
            i++;
        }
        // 对精细格网内的乡镇图斑循环，与地表覆盖图斑相交计算
        for(Tuple2<String,Iterable<Tuple2<String,Geometry>>> county:cells){
            if(!county._1().equals(stringIterableTuple2._1())){
                // 如果county的精细格网编码与当前计算的LCRA精细格网编码不一致，则继续循环
                continue;
            } else {
                // 如果county的精细格网编码与当前计算的LCRA精细格网编码一致，则进行计算，计算结束后跳出循环
                Iterator<Tuple2<String,Geometry>> countyIterator =  county._2().iterator();
                while(countyIterator.hasNext()){
                    Tuple2<String,Geometry> countyTuple = countyIterator.next();
                    Geometry countyGeo = countyTuple._2();
                    QuadTree.QuadTreeIterator lcraIterator = quadtree.getIterator(countyGeo,0);
                    int num = lcraIterator.next();
                    while(num >= 0){
                        String[] propertys = quadtreeMap.get(quadtree.getElement(num)).split("\t");
                        if(propertys.length==0){
                            System.out.println(" ============ PROPERTY IS BAD =============");
                            num = lcraIterator.next();
                            continue;
                        }
                        String wkt = propertys[1];
                        Geometry lcraGeo = GeometryEngine.geometryFromWkt(wkt, WktImportFlags.wktImportDefaults, Geometry.Type.Polygon);
                        // Geometry lcraGeo = quadtreeMap.get(num)._2();
                        // 计算重叠和重叠面积
                        Geometry intersects = GeometryEngine.intersect(lcraGeo,countyGeo,null);
                        double area = intersects.calculateArea2D(); // TODO 这个计算是不精确的
                        // double area = MeasureClass.EllipsoidArea(GeometryEngine.geometryToWkt(intersects,WktExportFlags.wktExportDefaults));
                        // 把行政区编码和CC码一起放进Key中
                        if (area > 0) {
                            try {
                                String key = countyTuple._1() + "#" + propertys[0].substring(12,16);
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
                    break;
                }
            }
        }
        return out.iterator();
    }
}
