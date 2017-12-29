package edu.zju.gis.spark.TransformationTool;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;

/**
 * Created by zhy on 2017/6/24.
 */
public class MdbFeatureStatistics {

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
//        System.out.println(mdbFileArr.length);
        List<File> mdbFileList = Arrays.asList(mdbFileArr);
        List<String> mdbNameList = new ArrayList<String>();
        for(File mdbFile:mdbFileList){
            mdbNameList.add(mdbFile.getName().substring(0,mdbFile.getName().length()-4));
//            System.out.println(mdbFile.getName());
        }
        System.out.println(mdbNameList.size());
        return mdbNameList;
    }

    /**
     * mdb批量 转换坐标系并转为wkt文件 上传至hdfs
     * @param params
     */
    public static void mdbFeatureStatistics(Map<String,String> params) throws IOException {
        String filePath = (String) params.get("filePath");
        String layerName = (String) params.get("layerName");
        String filetype = (String)params.get("filetype");

        JavaSparkContext sc = new JavaSparkContext();

        List<String> mdbNameList = getAllMdbFiles(filePath,filetype);
        if(mdbNameList.size() ==0){
            System.out.println("该文件夹中没有"+filetype+"文件！");
        }else {
            JavaRDD<String> mdbFileNameRDD = sc.parallelize(mdbNameList, mdbNameList.size());
            List<Tuple2<String, Iterable<String>>> resultInfo=null;
            List<Long> countInfo = null;
//            countInfo = mdbFileNameRDD.map(new featureCount(filePath, layerName, filetype)).collect();
//            for(Long l:countInfo)
//            {
//                System.out.println(l);
//            }
            Long sum = mdbFileNameRDD.map(new featureCount(filePath, layerName, filetype)).reduce(new Function2<Long, Long, Long>() {
                @Override
                public Long call(Long aLong, Long aLong2) throws Exception {
                    return aLong+aLong2;
                }
            });
            System.out.println(sum);
        }
    }


    static class featureCount implements Function<String,Long> {
        String filePath ;
        String layerName ;
        String filetype;

        featureCount(String filePath ,String layerName,String filetype){
            this.filePath = filePath;
            this.layerName = layerName;
            this.filetype = filetype;
        }

        @Override
        public Long call(String mdbName) throws Exception{
            String inputPath = filePath+"/"+mdbName+filetype;
            String mdbName_rc = mdbName.substring(mdbName.lastIndexOf("_") + 1);
            if (mdbName_rc.substring(0, 2).equals("51") || mdbName_rc.substring(0, 2).equals("65")) {
                MdbFeatureCount fc = new MdbFeatureCount();
                Long count = fc.mdbFeatureCount(inputPath, layerName);
                return count;
            }else{
                return Long.valueOf(0);
            }
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
        param_map.put("filePath", params.get("filepath"));
        param_map.put("layerName", params.get("layername"));
        param_map.put("filetype", params.get("filetype"));
        mdbFeatureStatistics(param_map);
    }
}
