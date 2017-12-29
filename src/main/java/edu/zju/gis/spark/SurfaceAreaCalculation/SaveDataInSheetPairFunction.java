package edu.zju.gis.spark.SurfaceAreaCalculation;

import com.esri.core.geometry.*;
import com.sun.jndi.toolkit.ctx.StringHeadTail;
import edu.zju.gis.gncstatistic.GridArchitectureFactory;
import edu.zju.gis.gncstatistic.GridArchitectureInterface;
import edu.zju.gis.gncstatistic.Sheet;
import edu.zju.gis.gncstatistic.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.BufferedOutputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;

/**
 * 将格网中的Geometry保存成wkt文件，属性数据保存成属性文件，四叉树索引保存成索引文件
 * Created by zhy on 2017/8/14.
 */
public class SaveDataInSheetPairFunction implements PairFunction<Tuple2<String,Iterable<Tuple2<Geometry,String>>>, String, String> {

    public String rootDir;
    public String hdfsname;
    GridArchitectureInterface gridArchitecture;

    public SaveDataInSheetPairFunction(String rootDir,String hdfsName,int gridSize,String gridType){
        this.rootDir = rootDir;
        this.hdfsname = hdfsName;
        gridArchitecture = GridArchitectureFactory.GetGridArchitecture(gridSize,gridType);
    }

    public Tuple2<String, String> call(Tuple2<String, Iterable<Tuple2<Geometry, String>>> longIterableTuple2) throws Exception {

        int i=0;
        boolean errorFlag = false;
        String errorlog = "";
        try {

            Configuration conf = new Configuration();
            conf.set("fs.default.name",this.hdfsname);

            Sheet sheet = new Sheet(longIterableTuple2._1());
            Envelope gridBoundary = gridArchitecture.GetSpatialRange(sheet);

            String rootDirectory = Utils.GetGridRootPath(sheet,rootDir);

            //创建输入流
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(new Path(rootDirectory))){
                fs.delete(new Path(rootDirectory),true);
            }
            fs.mkdirs(new Path(rootDirectory));

            FSDataOutputStream wktOutStream = fs.create(new Path(Utils.GetGeofilePath(sheet,rootDir)),true);
            FSDataOutputStream propertyOutStream = fs.create(new Path(Utils.GetPropertyfilePath(sheet,rootDir)),true);
            FSDataOutputStream quadtreeIndex = fs.create(new Path(Utils.GetSpatialIndexfilePath(sheet,rootDir)),true);

            BufferedOutputStream wktBufferedOutputStream = new BufferedOutputStream(wktOutStream);
            BufferedOutputStream propertyBufferedwktOutputStream = new BufferedOutputStream(propertyOutStream);
            ObjectOutputStream indexObjectOutputStream = new ObjectOutputStream(quadtreeIndex);


            QuadTree quadtree = new QuadTree(new Envelope2D(gridBoundary.getXMin(),gridBoundary.getYMin(),gridBoundary.getXMax(),gridBoundary.getYMax()),8);

            Iterator<Tuple2<Geometry, String>> iterator = longIterableTuple2._2.iterator();

            while (iterator.hasNext()){
                Tuple2<Geometry, String> item = iterator.next();
                Envelope2D envelope = new Envelope2D();
                item._1().queryEnvelope2D(envelope);

                String wkt = GeometryEngine.geometryToWkt(item._1,0);

                quadtree.insert(i,envelope);
                //在wkt里先输出图幅号  再输出code  然后是wkt
                wktBufferedOutputStream.write(((GetSheetCode(item._2))+"\t"+GetFeatureCode(item._2)+"\t"+wkt+"\r\n").getBytes());
                propertyBufferedwktOutputStream.write((item._2()+"\r\n").getBytes());
                i++;
            }
            indexObjectOutputStream.writeObject(quadtree);



            wktBufferedOutputStream.close();
            wktOutStream.close();

            propertyBufferedwktOutputStream.close();
            propertyOutStream.close();

            quadtreeIndex.close();

        }catch (Exception ex){
            errorFlag = true;

            errorlog = "failed:"+ex.toString()+","+QuadTree.class.getResource("");
        }
        if(errorFlag){
            return new Tuple2<String, String>(longIterableTuple2._1.toString(),errorlog);
        }else{
            return new Tuple2<String, String>(longIterableTuple2._1.toString(),Integer.toString(i));
        }
    }
    private String GetFeatureCode(String property){
        return property.split("\t")[0];
    }

    private String GetSheetCode(String property){
        String[] s = property.split("\t");
        int length = s.length;
        return s[length-1];
    }
}
