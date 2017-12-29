package edu.zju.gis.spark.ParallelTools.TransformFunctions;

import com.esri.core.geometry.*;
import edu.zju.gis.gncstatistic.Grid;
import edu.zju.gis.gncstatistic.GridArchitectureFactory;
import edu.zju.gis.gncstatistic.LatLonGridArchitecture;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Zhy on 2017/11/6.
 */
public class ChangeDetectMapFunction implements FlatMapFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>, String> {

    //key:gridCode  value:regioncode+"\t"+ccCode+"\t"+intersectWkt
    @Override
    public Iterator<String> call(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> stringTuple2Tuple2) throws Exception {
        List<String> out = new ArrayList<String>();
        Iterable<String> lcra1InCounty  = stringTuple2Tuple2._2()._1();  //格式为：regioncode+"\t"+ccCode+"\t"+intersectWkt
        Iterable<String> lcra2InGrid = stringTuple2Tuple2._2()._2();
        Iterator<String> lcra1InCountyIterator = lcra1InCounty.iterator();
        Iterator<String> lcra2InGridIterator = lcra2InGrid.iterator();
        String gridCode = stringTuple2Tuple2._1();
        Grid grid = Grid.Parse(gridCode);
        LatLonGridArchitecture countyGridArc = (LatLonGridArchitecture) GridArchitectureFactory.GetGridArchitecture(0.2D, "latlon");
        Envelope gridBoundary = countyGridArc.GetSpatialRange(grid);
        QuadTree lcra2Quadtree = new QuadTree(new Envelope2D(gridBoundary.getXMin(), gridBoundary.getYMin(), gridBoundary.getXMax(), gridBoundary.getYMax()), 8);
        //将lcra2InGrid存入四叉树中
        Map<Integer, String> lcra2QuadtreeMap = new HashMap<Integer, String>();
        int i = 0;
        while(lcra2InGridIterator.hasNext()){
            String items = lcra2InGridIterator.next();
            String item = items.split("\t")[2];
            if ((!item.startsWith("POLYGON")) && (!item.startsWith("MULTIPOLYGON"))) {
                System.out.println(" ===== THIS WKT IS WRONG IN LCRAMAP ======");
                System.out.println(item);
                continue;
            }
            Envelope2D envelope = new Envelope2D();
            Geometry itemGeo = GeometryEngine.geometryFromWkt(item, WktImportFlags.wktImportDefaults, Geometry.Type.Unknown);
            itemGeo.queryEnvelope2D(envelope);
            lcra2QuadtreeMap.put(i, items);
            lcra2Quadtree.insert(i, envelope);
            i++;
        }
        System.out.println("四叉树创建完成！");
        while (lcra1InCountyIterator.hasNext()){
            System.out.println("开始进行变化提取");
            String lcra1Items = lcra1InCountyIterator.next();
            String lcraWkt = lcra1Items.split("\t")[2];
            System.out.println(lcraWkt);
            String ccCode1 = lcra1Items.split("\t")[1];
            Geometry lcra1Geo = GeometryEngine.geometryFromWkt(lcraWkt, WktImportFlags.wktImportDefaults, Geometry.Type.Unknown);
            QuadTree.QuadTreeIterator lcra2Iterator = lcra2Quadtree.getIterator(lcra1Geo, 0);
            int num = lcra2Iterator.next();
            while(num>=0){
                System.out.println(num);
                String[] propertys = lcra2QuadtreeMap.get(lcra2Quadtree.getElement(num)).split("\t");
                String regionCode = propertys[0];
                String ccCode2 = propertys[1];
                String wkt = propertys[2];
                if(ccCode1.equals(ccCode2)){
                    num = lcra2Iterator.next();
                    continue;
                }
                Geometry lcra2Geo = GeometryEngine.geometryFromWkt(wkt, WktImportFlags.wktImportDefaults, Geometry.Type.Unknown);
                Geometry intersects = GeometryEngine.intersect(lcra1Geo, lcra2Geo, null);
                if(intersects.isEmpty()){
                    num = lcra2Iterator.next();
                    continue;
                }
                String intersectWkt = GeometryEngine.geometryToWkt(intersects,WktExportFlags.wktExportDefaults);
                out.add(regionCode+"\t"+ccCode1+"\t"+ccCode2+"\t"+intersectWkt);
                num = lcra2Iterator.next();
            }
        }
        return out.iterator();
    }
}
