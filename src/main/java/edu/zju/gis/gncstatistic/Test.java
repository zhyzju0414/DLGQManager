package edu.zju.gis.gncstatistic;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.QuadTree;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Created by Frank on 2017/6/14.
 */
public class Test {

    public static void main(String[] args) throws Exception {

//        File file = new File("C:\\Users\\Frank\\Desktop\\cehui\\xianjie20.txt");
//        InputStreamReader read = new InputStreamReader(new FileInputStream(file));
//        BufferedReader bufferedReader = new BufferedReader(read);
//        String lineTxt = bufferedReader.readLine().split("\t")[1];
//
//        Geometry geo = GeometryEngine.geometryFromWkt(lineTxt,0, Geometry.Type.Polygon);
//
//        File file1 = new File("C:\\Users\\Frank\\Desktop\\cehui\\equal_grid0.txt");
//        InputStreamReader read1 = new InputStreamReader(new FileInputStream(file1));
//        BufferedReader bufferedReader1 = new BufferedReader(read1);
//        String gridLineText = null;
//
//        FileOutputStream outputStream = new FileOutputStream(new File("C:\\Users\\Frank\\Desktop\\cehui\\result.txt"));
//        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);
//
//
//        List<Geometry> geoList = new ArrayList<Geometry>();
//
//        while ((gridLineText = bufferedReader1.readLine())!=null){
//            String wkt = gridLineText.split("\t")[1];
//            Geometry grid = GeometryEngine.geometryFromWkt(wkt,0, Geometry.Type.Polygon);
//
//            if(GeometryEngine.contains(geo,grid,null)){
//                String txt = "0\t"+wkt+"\r\n";
//                bufferedOutputStream.write(txt.getBytes());
//            }else if(GeometryEngine.overlaps(geo,grid,null)){
//                String txt = "1\t"+wkt+"\r\n";
//                bufferedOutputStream.write(txt.getBytes());
//                geoList.add(grid);
//            }else{
//                String txt = "2\t"+wkt+"\r\n";
//                bufferedOutputStream.write(txt.getBytes());
//            }
//
//
//        }
//        bufferedOutputStream.close();
//        bufferedReader1.close();
//
//        Geometry clipedGeo = GeometryEngine.intersect(geo,geoList.get(0),null);
//        System.out.println(clipedGeo.calculateArea2D());
//        Date d1 = new Date();
//        for(int i=0;i<10000;i++){
//            Geometry geo1 = GeometryEngine.intersect(clipedGeo,geo,null);
//           // System.out.println(geo1.calculateArea2D());
//        }
//        Geometry geo2 = GeometryEngine.intersect(clipedGeo,geo,null);
//        System.out.println(geo2.calculateArea2D());
//        Date d2 = new Date();
//
//        for(int i=0;i<10000;i++){
//            Geometry geo1 =  GeometryEngine.intersect(clipedGeo,clipedGeo,null);
//          //  System.out.println(geo1.calculateArea2D());
//        }
//        Geometry geo3 =  GeometryEngine.intersect(clipedGeo,clipedGeo,null);
//        System.out.println(geo3.calculateArea2D());
//        Date d3 = new Date();
//
//        System.out.println(d2.getTime()-d1.getTime());
//        System.out.println(d3.getTime()-d2.getTime());
//
//        QuadTree quadtree = new QuadTree(null,8);

       // HDFSOperator();
       // ReadObjectFromGDFS();

        List<String> test = new ArrayList<String>();
        test.add("48912");
        test.add("45254");
        test.add("64329");
        Collections.sort(test, Collator.getInstance(Locale.ENGLISH));
        for(String item:test){
            System.out.println(item);
        }

    }

    public static void HDFSOperator() throws IOException {

        File file1 = new File("C:\\Users\\Frank\\Desktop\\cehui\\equal_grid0.txt");
        InputStreamReader read1 = new InputStreamReader(new FileInputStream(file1));
        BufferedReader bufferedReader1 = new BufferedReader(read1);
        String gridLineText = null;


        QuadTree quadtree = new QuadTree(new Envelope2D(0,0,180,180),8);
        int i=0;
        while ((gridLineText = bufferedReader1.readLine())!=null){
            String wkt = gridLineText.split("\t")[1];
            Geometry grid = GeometryEngine.geometryFromWkt(wkt,0, Geometry.Type.Polygon);
            Envelope2D env = new Envelope2D();
            grid.queryEnvelope2D(env);
            quadtree.insert(i,env);

            i++;
        }
        bufferedReader1.close();







        FileInputStream in = null;
        OutputStream out = null;

        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://namenode:9000");

        FileSystem fs = FileSystem.get(conf);

        FSDataOutputStream fileOutStream = fs.create(new Path("hdfs://namenode:9000/zhaoxianwei/iiii"),true);
        ObjectOutputStream objectOutStream = new ObjectOutputStream(fileOutStream);
        objectOutStream.writeObject(quadtree);
        fileOutStream.close();


    }

    public static void ReadObjectFromGDFS() throws IOException, ClassNotFoundException {

        FileInputStream in = null;

        Configuration conf = new Configuration();
        conf.set("fs.default.name","hdfs://namenode:9000");

        FileSystem fs = FileSystem.get(conf);

        FSDataInputStream inputStream = fs.open(new Path("hdfs://namenode:9000/zhaoxianwei/iiii"));
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        QuadTree qt = (QuadTree)objectInputStream.readObject();
        //QuadTree.QuadTreeIterator qti =  qt.getIterator();
        int count = qt.getElementCount();
        System.out.println(count);

        for(int i=0;i<count;i++){
            System.out.println(qt.getElement(i));
        }



    }



}
