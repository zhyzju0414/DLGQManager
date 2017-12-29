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
public class MdbTransformBySpark {

    final public static String prj = "GEOGCS[\"GCS_China_Geodetic_Coordinate_System_2000\",DATUM[\"D_China_2000\",SPHEROID[\"CGCS2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433],METADATA[\"China\",73.62,16.7,134.77,53.55,0.0,0.0174532925199433,0.0,1067],AUTHORITY[\"EPSG\",4490]]";
//    final public static String filePath = "\\\\x410.ngcc.com\\GNCDATA\\DLGGC2016\\1";
//    final public static String filetype = ".gdb";
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
//                final String ft = filetype;
                if(pathname.getName().toLowerCase().endsWith(filetype)){
                    return true;
                }
                return false;
            }
        };
        File[] mdbFileArr = file.listFiles(fileFilter);
        System.out.println(filePath);
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
     */
    public static void convertShp2Wkt(String[] args) throws IOException {
        Map parameters = edu.zju.gis.spark.Utils.ParseParameters(args);
        String filePath =(String)parameters.get("filepath");
        String layerName =(String)parameters.get("layername");
        String outputFilePath = (String)parameters.get("outputfilepath");
        String hdfsName = (String)parameters.get("hdfsname");
        String fileType = (String)parameters.get("filetype");
        String IsLcraOrNot = (String)parameters.get("islcraornot");
        String trgPrjDef = (String)parameters.get("trgprjdef");

        JavaSparkContext sc = new JavaSparkContext();

        List<String> mdbNameList = getAllMdbFiles(filePath,fileType);
        if(mdbNameList.size() ==0){
            System.out.println("该文件夹中没有"+fileType+"文件！");
        }else {
            List<Tuple2<String, Iterable<String>>> resultInfo=null;

            if(IsLcraOrNot.equals("true")){
                JavaRDD<String> mdbFileNameRDD = sc.parallelize(mdbNameList, mdbNameList.size());
                //如果是转换地表覆盖数据  则以县为单位输出wkt文件
                resultInfo =
                        mdbFileNameRDD.map(new DataConvert(filePath, layerName, outputFilePath, fileType, trgPrjDef, hdfsName)).mapToPair(new PairFunction<String, String, String>() {
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
//                JavaRDD<String> mdbFileNameRDD = sc.parallelize(mdbNameList, mdbNameList.size());
                JavaRDD<String> mdbFileNameRDD = sc.parallelize(mdbNameList, 1);
                String outputPath = outputFilePath+"/"+"xiangzhen.txt";
                Configuration conf = new Configuration();
                conf.set("fs.default.name",hdfsName);
                FileSystem fs = FileSystem.get(conf);
                FSDataOutputStream wktOutStream = fs.create(new Path(outputPath),true);
                wktOutStream.close();
//                fs.close();
                resultInfo =
                        mdbFileNameRDD.map(new DataConvert2(filePath, layerName, outputFilePath, fileType, trgPrjDef, hdfsName)).mapToPair(new PairFunction<String, String, String>() {
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


    static class DataConvert2 implements Function<String, String> {

        String filePath ;
        String layerName ;
        String outputFilePath ;
        String trgPrjDef;
        String hdfsname;
        String filetype;
        String outputPath;

        DataConvert2(String filePath ,String layerName,String outputFilePath,String filetype,String trgPrjDef,String hdfsname){
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
            //只转换51 65
                Mdb2wkt test = new Mdb2wkt();
                String resultInfo =
                        test.mdb2wkt(inputPath, layerName, prj, wktOutStream);
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
            String outputPath = outputFilePath + "/" + mdbName + ".txt";
            String inputPath = filePath + "/" + mdbName + filetype;
//            String mdbName_rc = mdbName.substring(mdbName.lastIndexOf("_") + 1);
//            if (mdbName_rc.substring(0, 2).equals("51") | mdbName_rc.substring(0, 2).equals("65")) {
                Configuration conf = new Configuration();
                conf.set("fs.default.name", hdfsname);
                FileSystem fs = FileSystem.get(conf);
                FSDataOutputStream wktOutStream = fs.create(new Path(outputPath), true);
                Mdb2wkt test = new Mdb2wkt();
                String resultInfo =
                        test.mdb2wkt(inputPath, layerName, prj, wktOutStream);
                wktOutStream.close();
//            System.out.println("上传文件 "+mdbName+" 完成！");
                return resultInfo;
//            }
//            return "skipinfo";
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
//        OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
//        Map<String, String> param_map = new HashMap<String, String>();
//        param_map.put("filePath",params.get("filepath"));
//        param_map.put("layerName",params.get("layername"));
//        param_map.put("outputFilePath",params.get("outputfilepath"));
//        param_map.put("filetype",params.get("filetype"));
////        param_map.put("trgPrjDef",params.get("trgprjdef"));
//        param_map.put("hdfsname",params.get("hdfsname"));
//        param_map.put("IsLcraOrNot",params.get("islcraornot"));


        convertShp2Wkt(args);
    }
}
