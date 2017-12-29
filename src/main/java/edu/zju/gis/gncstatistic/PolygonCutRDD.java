package edu.zju.gis.gncstatistic;

import com.esri.core.geometry.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 复杂多边形裁切、统计
 * Created by Frank on 2017/6/16.
 */
public class PolygonCutRDD implements Serializable{

    public Polygon geo;

    public GridArchitectureInterface gridArchitecture;
    Map<String,Tuple2<Geometry,Integer>> gridMap;  //与多边形相交或包含的格网


    public PolygonCutRDD(JavaSparkContext ctx, Polygon geo, GridArchitectureInterface gridArchitecture){

        this.geo = geo;
        this.gridArchitecture = gridArchitecture;


        Envelope2D env = new Envelope2D();
        geo.queryEnvelope2D(env);
        Tuple2<Grid,Grid> gridRange = this.gridArchitecture.GetCellRowRange(env);

        //获取格网列表进一步过滤
        List<Grid> gridList = new ArrayList<Grid>();
        for(long i=gridRange._1().Row;i<=gridRange._2().Row;i++){
            for(long j=gridRange._1().Col;j<=gridRange._2.Col;j++){
                Grid grid = new Grid(i,j);
                gridList.add(grid);
            }
        }
        final Broadcast<Polygon> bGeo = ctx.broadcast(geo);
        final Broadcast<GridArchitectureInterface> bgridArchitecture = ctx.broadcast(gridArchitecture);

        //将格网分成相交格网和内部格网
        gridMap =
        ctx.parallelize(gridList,gridList.size()>100?gridList.size()/10:10).mapToPair(new PairFunction<Grid, String, Tuple2<Geometry,Integer>>() {
            @Override
            public Tuple2<String, Tuple2<Geometry, Integer>> call(Grid grid) throws Exception {
                Envelope gridEnv = bgridArchitecture.getValue().GetSpatialRange(grid);

                int flag = -1; //格网与多边形的空间关系标志，如格网在多边形内部，则为0，多边形边界与格网相交则为1，相离则为2
                Geometry clipGeo = null;
                if(GeometryEngine.contains(bGeo.getValue(),gridEnv,null)){
                    clipGeo = gridEnv;
                    flag = 0;
                }else if(GeometryEngine.overlaps(bGeo.getValue(),gridEnv,null)){
                    flag = 1;
                    clipGeo = GeometryEngine.intersect(bGeo.getValue(),gridEnv,null);
                }else{
                    flag = 2;
                }
                return new Tuple2<String, Tuple2<Geometry, Integer>>(grid.toString(),new Tuple2<Geometry, Integer>(clipGeo,flag));
            }
        }).filter(new Function<Tuple2<String, Tuple2<Geometry, Integer>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Tuple2<Geometry, Integer>> stringTuple2Tuple2) throws Exception {
                return stringTuple2Tuple2._2._2!=2;
            }
        }).collectAsMap();

    }

    /**
     * 计算和多边形相交的面积总和
     * @param ctx
     * @return
     */
    public double GetIntersectArea(JavaSparkContext ctx){

        List<Grid>  boundaryGrid = new ArrayList<Grid>();
        List<Grid>  innerGrid = new ArrayList<Grid>();
        for (Map.Entry<String,Tuple2<Geometry,Integer>> item:gridMap.entrySet()
             ) {
            if(item.getValue()._2==1){
                boundaryGrid.add(Grid.Parse(item.getKey()));
            }else{
                innerGrid.add(Grid.Parse(item.getKey()));
            }
        }
        JavaPairRDD<String,Iterable<String>> boundaryGridRDD = Utils.GetSpatialRDD(ctx,boundaryGrid);
        JavaPairRDD<String,Iterable<String>> propertyGridRDD = Utils.GetPropertyRDD(ctx,innerGrid);

        double boundaryArea =
        boundaryGridRDD.map(new CalculateArea(geo)).reduce(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble+aDouble2;
            }
        });

        double innerArea = 0.0;
        if(propertyGridRDD!=null){
            innerArea =
                    propertyGridRDD.map(new Function<Tuple2<String,Iterable<String>>, Double>() {
                        @Override
                        public Double call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
                            double area = 0.0;
                            Iterator<String> iterator = stringIterableTuple2._2.iterator();
                            while (iterator.hasNext()){
                                String property = iterator.next();
                                String[] arr = property.split("\t");
                                area+=Double.parseDouble(arr[arr.length-2]);
                            }
                            return area;
                        }
                    }).reduce(new Function2<Double, Double, Double>() {
                        @Override
                        public Double call(Double aDouble, Double aDouble2) throws Exception {
                            return aDouble+aDouble2;
                        }
                    });
        }


        return boundaryArea+innerArea;
    }

    public List<String> GetInetrsectID(JavaSparkContext ctx){

        List<Grid>  boundaryGrid = new ArrayList<Grid>();
        List<Grid>  innerGrid = new ArrayList<Grid>();
        for (Map.Entry<String,Tuple2<Geometry,Integer>> item:gridMap.entrySet()
                ) {
            if(item.getValue()._2==1){
                boundaryGrid.add(Grid.Parse(item.getKey()));
            }else{
                innerGrid.add(Grid.Parse(item.getKey()));
            }
        }
        JavaPairRDD<String,Iterable<String>> boundaryGridRDD = Utils.GetSpatialRDD(ctx,boundaryGrid);
        JavaPairRDD<String,Iterable<String>> propertyGridRDD = Utils.GetPropertyRDD(ctx,innerGrid);

        JavaRDD<String> intersectRDD = boundaryGridRDD.flatMap(new CalculateIntersect(geo));
        List<String> innerList = null;
        if(propertyGridRDD!=null){
           JavaRDD<String> innerRDD = propertyGridRDD.flatMap(new FlatMapFunction<Tuple2<String,Iterable<String>>, String>() {
                @Override
                public Iterator<String> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
                    List<String> results = new ArrayList<String>();

                    Iterator<String> iterator = stringIterableTuple2._2.iterator();
                    while (iterator.hasNext()){
                        String property = iterator.next();
                        String[] arr = property.split("\t");
                        results.add(arr[0]);
                    }
                    return results.iterator();
                }
            });
            intersectRDD = intersectRDD.union(innerRDD);
        }


       return intersectRDD.collect();

    }



    class CalculateArea implements Function<Tuple2<String,Iterable<String>>, Double>{


        public Polygon geo;

        public CalculateArea(Polygon geo){

            this.geo = geo;
        }

        @Override
        public Double call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {

            QuadTree quadTree = Utils.GetQuadTree(Grid.Parse(stringIterableTuple2._1));
            List<Geometry> geoList = new ArrayList<Geometry>();
            double area = 0.0;
            Iterator<String> iterator =
            stringIterableTuple2._2.iterator();

            while (iterator.hasNext()){
                String item = iterator.next();
                String wkt = item.split("\t")[1];
                Geometry geo = GeometryEngine.geometryFromWkt(wkt,0, Geometry.Type.Polygon);
                geoList.add(geo);
            }

            Envelope2D envelope = new Envelope2D();
            geo.queryEnvelope2D(envelope);

            QuadTree.QuadTreeIterator quadTreeIterator = quadTree.getIterator(envelope,0);
            int elementhandel = quadTreeIterator.next();
            while (elementhandel>=0){
                int featureIndex = quadTree.getElement(elementhandel);
                Geometry clipGeo = GeometryEngine.intersect(geo,geoList.get(featureIndex),null);
                if(!clipGeo.isEmpty()){
                    area+=clipGeo.calculateArea2D();
                }

                elementhandel = quadTreeIterator.next();
            }


            return area;
        }
    }

    class CalculateIntersect implements FlatMapFunction<Tuple2<String,Iterable<String>>, String>{


        public Polygon geo;

        public CalculateIntersect(Polygon geo){

            this.geo = geo;
        }

        @Override
        public Iterator<String> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {

            List<String> results = new ArrayList<String>();

        //    QuadTree quadTree = Utils.GetQuadTree(Grid.Parse(stringIterableTuple2._1));
        //    List<Tuple2<String,Geometry>> geoList = new ArrayList<Tuple2<String,Geometry>>();

            Iterator<String> iterator =
                    stringIterableTuple2._2.iterator();

            while (iterator.hasNext()){
                String item = iterator.next();
                String[] arr = item.split("\t");
                String ID = arr[0];
                String wkt = arr[1];
                Geometry geo1 = GeometryEngine.geometryFromWkt(wkt,0, Geometry.Type.Polygon);
              //  geoList.add(new Tuple2<String, Geometry>(ID,geo));
                Geometry clipGeo = GeometryEngine.intersect(geo,geo1,null);
                if(!clipGeo.isEmpty()){
                    results.add(item);
                }

            }

//            Envelope2D envelope = new Envelope2D();
//            geo.queryEnvelope2D(envelope);

//            QuadTree.QuadTreeIterator quadTreeIterator = quadTree.getIterator(envelope,0);
//            int elementhandel = quadTreeIterator.next();
//            while (elementhandel>=0){
//                int featureIndex = quadTree.getElement(elementhandel);
//             //   Geometry clipGeo = GeometryEngine.intersect(geo,geoList.get(featureIndex)._2,null);
//             //   if(!clipGeo.isEmpty()){
//                    results.add(geoList.get(featureIndex)._1);
//             //   }else if(GeometryEngine.contains(geo,geoList.get(featureIndex)._2,null)){
//             //       results.add(geoList.get(featureIndex)._1);
//             //   }
//
//                elementhandel = quadTreeIterator.next();
//            }


            return results.iterator();
        }


    }





}
