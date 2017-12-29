package edu.zju.gis.gncstatistic;

import com.esri.core.geometry.QuadTree;
//import com.sun.tools.corba.se.idl.StringGen;
import edu.zju.gis.spark.Common.CommonSettings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.gdal.osr.CoordinateTransformation;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Frank on 2017/6/15.
 */
public class Utils {


//    public static String rootDir = "hdfs://namenode:9000/hls_all";   //根路径

    public static String DEFAULT_OUTPUT_DELIMITER = "\t";//输出文件的默认分隔符

    final public static String GEO_FILENAME = "wkt";  //wkt文件名

    final public static String PROPERTY_FILENAME = "property";  //属性文件名

    final public static String SPATIAL_INDEX_FILENAME = "quadtreeIndex";  //四叉树索引文件名

    static {

        Configuration conf = new Configuration();
//        try {
//            //conf.addResource(Utils.class.getClassLoader().getResource("hdfs-site.xml").openStream());
//           // rootDir = conf.get("gnc.rootdir");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }

    /**
     * 获取格网的空间文件路径
     * @param grid
     * @return
     */
    public static String GetGeofilePath(Grid grid){
        return GetGridRootPath(grid)+"/"+GEO_FILENAME;
    }
    public static String GetGeofilePath(Sheet sheet,String rootDir){
        return GetGridRootPath(sheet,rootDir)+"/"+GEO_FILENAME;
    }


    /**
     * 获取格网的属性文件路径
     * @param grid
     * @return
     */
    public static String GetPropertyfilePath(Grid grid){
        return GetGridRootPath(grid)+"/"+PROPERTY_FILENAME;
    }

    public static String GetPropertyfilePath(Sheet sheet,String rootDir){
        return GetGridRootPath(sheet,rootDir)+"/"+PROPERTY_FILENAME;
    }
    /**
     * 获取格网的四叉树索件文件路径
     * @param grid
     * @return
     */
    public static String GetSpatialIndexfilePath(Grid grid){
        return GetGridRootPath(grid)+"/"+SPATIAL_INDEX_FILENAME;
    }
    public static String GetSpatialIndexfilePath(Sheet sheet, String rootDir){
        return GetGridRootPath(sheet,rootDir)+"/"+SPATIAL_INDEX_FILENAME;
    }
    /**
     * 获取格网的根路径
     * @param grid
     * @return
     */
    public static String GetGridRootPath(Grid grid){
        return CommonSettings.DEFAULT_GRID_DIR+"/"+grid.Col+"/"+grid.Row;
    }

    public static String GetGridRootPath(Sheet sheet,String rootDir){
        return rootDir+"/"+sheet.toString().substring(0,3)+"/"+sheet.toString();
    }
    /**
     * 获取格网属性RDD
     * @param ctx
     * @param gridList
     * @return
     */
    public static JavaPairRDD<String,Iterable<String>> GetPropertyRDD(JavaSparkContext ctx,List<Grid> gridList){
        if(gridList.size()<1){
            return null;
        }
        JavaPairRDD<String,Iterable<String>> firstRDD =
        ctx.textFile(GetPropertyfilePath(gridList.get(0))).mapToPair(new WholePairFunction(gridList.get(0))).groupByKey();

        List<JavaPairRDD<String,Iterable<String>>> listRDD = new ArrayList<JavaPairRDD<String, Iterable<String>>>();
        for(int i=1;i<gridList.size();i++){
            listRDD.add(ctx.textFile(GetPropertyfilePath(gridList.get(i))).mapToPair(new WholePairFunction(gridList.get(i))).groupByKey());
        }
        return ctx.union(firstRDD,listRDD);
    }

    /**
     * 功能类似于getSpatialRDD，只是把CC码也一起放到了key里
     * @param ctx
     * @param gridList
     * @return
     */
    public static JavaPairRDD<String,Iterable<String>> GetCCSpatialRDD(JavaSparkContext ctx,List<Grid> gridList){

        if(gridList.size()<1){
            return null;
        }

        int firstGridIndex = -1;

        List<JavaPairRDD<String,Iterable<String>>> listRDD = new ArrayList<JavaPairRDD<String, Iterable<String>>>();

        for(int i=0;i<gridList.size();i++){
            Grid fGrid = gridList.get(i);
            long zCode = Long.valueOf(fGrid.toString());
            if(CountyTest.listDataFiles.containsKey(zCode)){
                if(firstGridIndex==-1){
                    firstGridIndex = i;
                }else{
                    listRDD.add(ctx.textFile(GetGeofilePath(gridList.get(i))).mapToPair(new WholePairFunctionCC(gridList.get(i))).groupByKey(64));
                }
            }
        }

        if(firstGridIndex==-1){
            return null;
        } else {
            JavaPairRDD<String,Iterable<String>> firstRDD =  ctx.textFile(GetGeofilePath(gridList.get(firstGridIndex))).mapToPair(new WholePairFunctionCC(gridList.get(firstGridIndex))).groupByKey(64);
            return ctx.union(firstRDD,listRDD);
        }
    }

    /**
     * 获取格网空间RDD
     * @param ctx
     * @param gridList
     * @return
     */
    public static JavaPairRDD<String,Iterable<String>> GetSpatialRDD(JavaSparkContext ctx,List<Grid> gridList){
        if(gridList.size()<1){
            return null;
        }

        int firstGridIndex = -1;

        List<JavaPairRDD<String,Iterable<String>>> listRDD = new ArrayList<JavaPairRDD<String, Iterable<String>>>();
        for(int i=0;i<gridList.size();i++){
            Grid fGrid = gridList.get(i);
            long zCode = Long.valueOf(fGrid.toString());
//            System.out.println(" ===================================================================== ");
//            for(Long key:CountyTest.listDataFiles.keySet()){
//                System.out.println(key);
//            }
//            System.out.println(" ====== ZCODE:" + zCode);
            if(CountyTest.listDataFiles.containsKey(zCode)){
                if(firstGridIndex==-1){
                    firstGridIndex = i;
                }else{
                    listRDD.add(ctx.textFile(GetGeofilePath(gridList.get(i))).mapToPair(new WholePairFunction(gridList.get(i))).groupByKey(64));
                }
            }
        }
        if(firstGridIndex==-1){
            return null;
        } else {
            JavaPairRDD<String,Iterable<String>> firstRDD =  ctx.textFile(GetGeofilePath(gridList.get(firstGridIndex))).mapToPair(new WholePairFunction(gridList.get(firstGridIndex))).groupByKey(64);
            return ctx.union(firstRDD,listRDD);
        }
    }


    /**
     * 读取格网的四叉树对象
     * @param grid
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static QuadTree GetQuadTree(Grid grid) throws IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.addResource(Utils.class.getClassLoader().getResource("hdfs-site.xml").openStream());

        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(new Path(GetSpatialIndexfilePath(grid)));
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        QuadTree qt = (QuadTree)objectInputStream.readObject();

        objectInputStream.close();
        inputStream.close();
        return qt;
    }



    static class WholePairFunction implements PairFunction<String, String, String>{

        Grid grid;

        public WholePairFunction(Grid grid){
            this.grid = grid;
        }

        @Override
        public Tuple2<String, String> call(String s) throws Exception {
            return new Tuple2<String, String>(grid.toString(),s);
        }
    }

    /**
     * 功能类似于WholePairFunction
     * 只是把CC码放进了key里
     */
    static class WholePairFunctionCC implements PairFunction<String, String, String>{

        Grid grid;

        public WholePairFunctionCC(Grid grid){
            this.grid = grid;
        }

        @Override
        public Tuple2<String, String> call(String s) throws Exception {
            // 将CC码放进数据中
            String cc;
            String s0 = s.split("\t")[0];
            if(s0.length()>16){
                cc = s0.substring(12,16);
                return new Tuple2<String, String>(grid.toString()+"#"+cc,s);
            } else {
                System.out.println(" ======== THIS PROPERTY OF WKT IS BAD ========");
                System.out.println(s);
            }

            return new Tuple2<String, String>("ERROR"+"#"+"0000",s); //TODO 把这个数据错误处理掉
        }
    }

    public static CoordinateTransformation ProjTransform(String source,String target){
//        String source = "GEOGCS[\"GCS_China_Geodetic_Coordinate_System_2000\",DATUM[\"D_China_2000\",SPHEROID[\"CGCS2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433],METADATA[\"China\",73.62,16.7,134.77,53.55,0.0,0.0174532925199433,0.0,1067],AUTHORITY[\"EPSG\",4490]]";
//        String target = "PROJCS[\"CGCS2000_3_Degree_GK_CM_117E\",GEOGCS[\"GCS_China_Geodetic_Coordinate_System_2000\",DATUM[\"D_China_2000\",SPHEROID[\"CGCS2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Gauss_Kruger\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",117.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0],AUTHORITY[\"EPSG\",4548]]";
        org.gdal.osr.SpatialReference sourceProj = new org.gdal.osr.SpatialReference();
        org.gdal.osr.SpatialReference targetProj = new org.gdal.osr.SpatialReference();
        java.util.Vector vector1 = new java.util.Vector();
        vector1.add(source);
        sourceProj.ImportFromESRI(vector1);
        java.util.Vector vector2 = new java.util.Vector();
        vector2.add(target);
        targetProj.ImportFromESRI(vector2);
        CoordinateTransformation coordinateTransformation = new CoordinateTransformation(sourceProj,targetProj);
        return coordinateTransformation;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(GetSpatialIndexfilePath(new Grid(1,2)));
    }
}
