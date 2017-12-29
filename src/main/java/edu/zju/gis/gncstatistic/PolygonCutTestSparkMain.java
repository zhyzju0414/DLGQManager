package edu.zju.gis.gncstatistic;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Polygon;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

/**
 * 多边形裁切实验
 * Created by Frank on 2017/6/15.
 * args[0] 多边形文件路径
 */
public class PolygonCutTestSparkMain {

    public static void main(String[] args) throws Exception {

        System.out.println(Utils.GetGridRootPath(new Grid(1,2)));

        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://namenode:9000");
        FileSystem fs = FileSystem.get(conf);

        FSDataInputStream inputStream = fs.open(new Path("hdfs://namenode:9000/zhaoxianwei/querypolygon0.txt"));

        InputStreamReader fileReader = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(fileReader);
        String wkt = reader.readLine().split("\t")[1];
        inputStream.close();

        Geometry geo = GeometryEngine.geometryFromWkt(wkt,0, Geometry.Type.Polygon);
        Envelope2D env = new Envelope2D();
        geo.queryEnvelope2D(env);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("ImportDataSparkMain");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        LatLonGridArchitecture latlonArchitecture = new LatLonGridArchitecture(0.2);
        PolygonCutRDD polygonCutRDD = new PolygonCutRDD(ctx,(Polygon) geo,latlonArchitecture);
       // double totalArea = polygonCutRDD.GetIntersectArea(ctx);
        List<String> list = polygonCutRDD.GetInetrsectID(ctx);

//
        for(int i=0;i<list.size();i++){
            System.out.println(list.get(i));
        }

    }

}
