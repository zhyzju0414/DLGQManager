package edu.zju.gis.spark.TransformationTool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;

/**
 * Created by zhy on 2017/6/24.
 */
public class MdbTransform2LocalBySpark {

    final public static String prj = "GEOGCS[\"GCS_China_Geodetic_Coordinate_System_2000\",DATUM[\"D_China_2000\",SPHEROID[\"CGCS2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433],METADATA[\"China\",73.62,16.7,134.77,53.55,0.0,0.0174532925199433,0.0,1067],AUTHORITY[\"EPSG\",4490]]";

    /**
     * 获取共享文件夹下所有mdbFile名称
     * @param filePath mdb文件共享文件夹路径
     * @return
     */
    public static List<String> getAllMdbFiles(String filePath, final String filetype){
        //获取共享文件夹下所有mdb文件
        File file = new File(filePath);
        FileFilter fileFilter = new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if(pathname.getName().toLowerCase().endsWith(filetype)){
                    return true;
                }
                return false;
            }
        };
        File[] mdbFileArr = file.listFiles(fileFilter);
        System.out.println(mdbFileArr.length);
        List<File> mdbFileList = Arrays.asList(mdbFileArr);
        List<String> mdbNameList = new ArrayList<String>();
        for(File mdbFile:mdbFileList){
            mdbNameList.add(mdbFile.getName().substring(0,mdbFile.getName().length()-4));
            System.out.println(mdbFile.getName());
        }
        System.out.println(mdbNameList.size());
        return mdbNameList;
    }

    /**
     * mdb批量 转换坐标系并转为wkt文件 上传至hdfs
     * @param params
     */
    public static void mdbTransform(Map<String,String> params) throws IOException {
        String filePath = (String) params.get("filePath");
        String layerName = (String) params.get("layerName");
        String outputFilePath = (String)params.get("outputFilePath");
        String filetype = (String)params.get("filetype");
        String trgPrjDef= (String)params.get("trgPrjDef");
        String hdfsname = (String)params.get("hdfsname");
        String IsLcraOrNot = (String)params.get("IsLcraOrNot");

        JavaSparkContext sc = new JavaSparkContext();

        List<String> mdbNameList = getAllMdbFiles(filePath,filetype);
        if(mdbNameList.size() ==0){
            System.out.println("该文件夹中没有"+filetype+"文件！");
        }else {
            List<Tuple2<String, Iterable<String>>> resultInfo=null;

            if(IsLcraOrNot.equals("true")){
                JavaRDD<String> mdbFileNameRDD = sc.parallelize(mdbNameList, mdbNameList.size());
                //如果是转换地表覆盖数据  则以县为单位输出wkt文件
                resultInfo =
                        mdbFileNameRDD.map(new DataConvert(filePath, layerName, outputFilePath, filetype, trgPrjDef, hdfsname)).mapToPair(new PairFunction<String, String, String>() {
                            @Override
                            public Tuple2<String, String> call(String s) throws Exception {
                                if (s.startsWith("S")) {
                                    return new Tuple2<String, String>("S", "1");
                                } else {
                                    return new Tuple2<String, String>("E", s);
                                }
                            }
                        }).groupByKey().collect();
            }else{
                //如果转为乡镇数据  则输出在同一个文件中
                JavaRDD<String> mdbFileNameRDD = sc.parallelize(mdbNameList, 1);
                String outputPath = outputFilePath+"/"+"xiangzhen.txt";
                Configuration conf = new Configuration();
                conf.set("fs.default.name",hdfsname);
                FileSystem fs = FileSystem.get(conf);
                FSDataOutputStream wktOutStream = fs.create(new Path(outputPath),true);
                wktOutStream.close();
//                fs.close();
                resultInfo =
                        mdbFileNameRDD.map(new DataConvert2Local(filePath, layerName, outputFilePath, filetype, trgPrjDef, hdfsname)).mapToPair(new PairFunction<String, String, String>() {
                            @Override
                            public Tuple2<String, String> call(String s) throws Exception {
                                if (s.startsWith("S")) {
                                    return new Tuple2<String, String>("S", "1");
                                } else {
                                    return new Tuple2<String, String>("E", s);
                                }
                            }
                        }).groupByKey().collect();
            }

            int errorCount = 0;
            for (Tuple2<String, Iterable<String>> item : resultInfo
                    ) {
                if (item._1.equals("E")) {
                    errorCount = 0;
                    Iterator<String> iterable =
                            item._2.iterator();
                    while (iterable.hasNext()) {
                        errorCount++;
                        System.out.println(iterable.next());
                    }
                }
            }
            System.out.println(errorCount);
        }
//        Long count = mdbFileNameRDD.map(new DataConvert(filePath,layerName,outputFilePath,trgPrjDef,hdfsname)).count();
//        System.out.println(count);
    }


    static class DataConvert2Local implements Function<String, String> {

        String filePath ;
        String layerName ;
        String outputFilePath ;
        String trgPrjDef;
        String hdfsname;
        String filetype;
        String outputPath;

        DataConvert2Local(String filePath ,String layerName,String outputFilePath,String filetype,String trgPrjDef,String hdfsname){
            this.filePath = filePath;
            this.layerName = layerName;
            this.outputFilePath = outputFilePath;
            this.trgPrjDef = trgPrjDef;
            this.hdfsname = hdfsname;
            this.filetype = filetype;
            this.outputPath = outputFilePath+"/"+"xiangzhen.txt";
        }


        @Override
        public String call(String mdbName) throws Exception {
//            String outputPath = outputFilePath+"/"+mdbName+".txt";
            String inputPath = filePath+"/"+mdbName+filetype;

            Configuration conf = new Configuration();
            conf.set("fs.default.name",hdfsname);
            conf.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER");
            conf.set("dfs.client.block.write.replace-datanode-on-failure.enable","true");
            FileSystem fs = FileSystem.get(conf);
            FSDataOutputStream wktOutStream = fs.append(new Path(outputPath));
            Mdb2wkt test = new Mdb2wkt();
            String resultInfo =
                    test.mdb2wkt(inputPath,layerName,prj,wktOutStream);
            wktOutStream.close();
            fs.close();
//            System.out.println("上传文件 "+mdbName+" 完成！");
            return resultInfo;
        }
    }
   static class DataConvert implements Function<String, String> {

        String filePath ;
        String layerName ;
        String outputFilePath ;
        String trgPrjDef;
        String hdfsname;
        String filetype;

        DataConvert(String filePath ,String layerName,String outputFilePath,String filetype,String trgPrjDef,String hdfsname){
            this.filePath = filePath;
            this.layerName = layerName;
            this.outputFilePath = outputFilePath;
            this.trgPrjDef = trgPrjDef;
            this.hdfsname = hdfsname;
            this.filetype = filetype;
        }


        @Override
        public String call(String mdbName) throws Exception {
            String outputPath = outputFilePath+"/"+mdbName+".txt";
            String inputPath = filePath+"/"+mdbName+filetype;

            Configuration conf = new Configuration();
            conf.set("fs.default.name",hdfsname);

            FileSystem fs = FileSystem.get(conf);
            FSDataOutputStream wktOutStream = fs.create(new Path(outputPath),true);
            Mdb2wkt test = new Mdb2wkt();
            String resultInfo =
            test.mdb2wkt(inputPath,layerName,prj,wktOutStream);
            wktOutStream.close();
//            System.out.println("上传文件 "+mdbName+" 完成！");
            return resultInfo;
        }
    }

    public static void main(String[] args) throws IOException {
//        args = new String[6];
//        args[0] = "filepath:D:\\zhy\\mdb2gdb";
//        args[1] = "layername:DLTB";
//        args[2] = "outputfilepath:D:\\";
//        args[3] = "filetype:.txt";
//        args[4] = "trgprjdef:null";
//        args[5] = "hdfsname:hdfs://namenode:9000";
//        args[6] = "islcraornot:true";
        OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
        Map<String, String> param_map = new HashMap<String, String>();
        param_map.put("filePath",params.get("filepath"));
        param_map.put("layerName",params.get("layername"));
        param_map.put("outputFilePath",params.get("outputfilepath"));
        param_map.put("filetype",params.get("filetype"));
//        param_map.put("trgPrjDef",params.get("trgprjdef"));
        param_map.put("hdfsname",params.get("hdfsname"));
        param_map.put("IsLcraOrNot",params.get("islcraornot"));
        mdbTransform(param_map);
//        String path ="2700\t632725\t囊谦县\n" +
//                "\t0.00000000000\t青海\t玉树藏族自治州\t";
//        path =replaceBlank(path);
    }
}
