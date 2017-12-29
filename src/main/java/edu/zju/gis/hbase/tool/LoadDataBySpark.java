package edu.zju.gis.hbase.tool;

import com.esri.core.geometry.GeometryEngine;
import com.sun.media.sound.SoftMixingDataLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.htrace.fasterxml.jackson.core.SerializableString;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.gdal.gdal.gdal;
import org.gdal.ogr.Geometry;
import org.gdal.osr.CoordinateTransformation;
import scala.Tuple2;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhy on 2017/8/29.
 * 用spark将数据批量导入HBASE
 */
public class LoadDataBySpark {
    public static void main(String[] args) throws IOException {
//        Configuration conf = HBaseConfiguration.create();
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("LoadDataBySpark");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

//        System.out.println(conf.get("hbase.zookeeper.quorum"));
//        System.out.println(conf.get("hbase.rootdir"));
        String lcraPath = args[0];
        String tablename = args[1];
        String code = args[2];
        final Broadcast<String> tablenameBC = sc.broadcast(tablename);
        List<String> lcraFileList = getLcraFiles(lcraPath,code);
        for(String name:lcraFileList){
            System.out.println(name);
        }
        //获得hdfs上lcra目录下所有的txt
        JavaPairRDD<String, String> tmp = sc.parallelize(lcraFileList, lcraFileList.size()).mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) {
                String tablename = tablenameBC.getValue();
                Configuration conf = HBaseConfiguration.create();
                try {
                    HTable tbl = new HTable(conf, tablename);
                    HTable tblIndex = new HTable(conf, Utils.GetSpatialIndexTableNameByDataTableName(tablename));
                    FileSystem fileSystem = FileSystem.get(URI.create(s), new Configuration());
                    FSDataInputStream fsDataInputStream = fileSystem.open(new Path(s));
                    InputStreamReader isr = new InputStreamReader(fsDataInputStream);
                    BufferedReader reader = new BufferedReader(isr);

                    String tempString = null;
                    int line = 1;

                    List<Put> putlst = new ArrayList();
                    List<Put> indexputlst = new ArrayList();
                    // 一次读入一行，直到读入null为文件结束
                    while ((tempString = reader.readLine()) != null) {
                        // 显示行号
                        System.out.println(line);
                        String[] values = tempString.split(Utils.DEFAULT_INPUT_DELIMITER);
                        String[] new_values = changeValueFormat(values);
                        Tuple2<Put, Put> putTuple = Utils.getRecordPut(new_values, 20151025);
                        putlst.add(putTuple._1);
                        indexputlst.add(putTuple._2);
                        if (putlst.size() == 1000) {
                            tbl.put(putlst);
                            tblIndex.put(indexputlst);
                            putlst.clear();
                            indexputlst.clear();
                        }
                        line++;
                    }
                    if (putlst.size() > 0) {
                        tbl.put(putlst);
                        tblIndex.put(indexputlst);
                        putlst.clear();
                        indexputlst.clear();
                    }
                    reader.close();
                } catch (Exception e) {
                    return new Tuple2<String, String>("error", e.getMessage());
                }
                return new Tuple2<String, String>("success","导入成功");
            }
        });

        //收集错误日志
        Map<String,String> resultList = tmp.collectAsMap();
        for(Map.Entry<String,String> entry:resultList.entrySet()){
            if(entry.getKey().equals("error")){
                System.out.println("error"+entry.getValue());
            }
        }

        System.out.println("导入完成");

    }

    /**
     * 进行格式转换时，要进行投影转换，并且重新计算长度和面积
     * @param line1Array
     * @return
     */
    public static String[] changeValueFormat(String[] line1Array) {
        gdal.AllRegister();
        String source ="GEOGCS[\"GCS_China_Geodetic_Coordinate_System_2000\",DATUM[\"D_China_2000\",SPHEROID[\"CGCS2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433],AUTHORITY[\"EPSG\",4490]]";
//        String target = "PROJCS[\"WGS_1984_Web_Mercator\",GEOGCS[\"GCS_WGS_1984_Major_Auxiliary_Sphere\",DATUM[\"D_WGS_1984_Major_Auxiliary_Sphere\",SPHEROID[\"WGS_1984_Major_Auxiliary_Sphere\",6378137.0,0.0]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Mercator\"],PARAMETER[\"false_easting\",0.0],PARAMETER[\"false_northing\",0.0],PARAMETER[\"central_meridian\",0.0],PARAMETER[\"standard_parallel_1\",0.0],UNIT[\"Meter\",1.0],AUTHORITY[\"EPSG\",3785]]";

        String[] newLineArray = new String[11];
        newLineArray[0] = "OBJECTID:" + Integer.parseInt(line1Array[0].substring(line1Array[0].length() - 8));
        newLineArray[1] = "CC:" + line1Array[0].substring(12, 16);
        newLineArray[2] = "TAG:" + line1Array[1];
//        newLineArray[3] = line1Array[8];//length
//        newLineArray[4] = line1Array[9];  //area
        newLineArray[5] = Integer.parseInt(line1Array[0].substring(line1Array[0].length() - 8)) + "";//objectid
        newLineArray[6] = line1Array[0].substring(0, 12);//12位行政区划编码
        newLineArray[7] = line1Array[0].substring(12, 16);//cc
        newLineArray[8] = line1Array[0].substring(12, 16);//cc
        newLineArray[9] = line1Array[11].substring(0, line1Array[11].indexOf(" "));//polygon
        newLineArray[10] = line1Array[11];//wkt
        Geometry geo = Geometry.CreateFromWkt(line1Array[11]);
        //由中心点获得该要素所在的经度 并且获得该要素所需要的投影文件
        int centralMeridian = getCentralMeridian(geo.Centroid().GetX());
//        System.out.println(newLineArray[6]+" "+newLineArray[0]);
//        System.out.println("中心点位置坐标为 "+geo.Centroid().GetX());
//        System.out.println("中央经线为 :"+centralMeridian);
        CoordinateTransformation ct = edu.zju.gis.gncstatistic.Utils.ProjTransform(source, edu.zju.gis.spark.Utils.targetProjMap.get(centralMeridian));
        geo.Transform(ct);
        newLineArray[4] = String.valueOf(geo.Area());   //面积
        com.esri.core.geometry.Geometry geometry = GeometryEngine.geometryFromWkt(geo.ExportToWkt(),0, com.esri.core.geometry.Geometry.Type.Unknown);
        newLineArray[3] = String.valueOf(geometry.calculateLength2D());  //长度
        return newLineArray;
    }

    //获得hdfs路径下面所有txt文件的完整路径列表
    public static List<String> getLcraFiles(String lcraPath,String code) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(lcraPath), new Configuration());
        Path path = new Path(lcraPath);
        FileStatus[] fileStatusesArray = fileSystem.listStatus(path);
        List<String> list_filename = new ArrayList<String>();
        for (FileStatus fileStatus : fileStatusesArray) {
            if(fileStatus.getPath().getName().startsWith("JC16_"+code)){
                list_filename.add(fileStatus.getPath().toString());
            }
        }
        return list_filename;
    }

    public static int getCentralMeridian(double meridian){
        meridian = meridian+6;
        int k = (int)(meridian/6);
        k =k*6-3;
        return k;
    }
}
