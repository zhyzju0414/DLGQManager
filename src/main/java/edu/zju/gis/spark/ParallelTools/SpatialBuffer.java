package edu.zju.gis.spark.ParallelTools;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.SpatialReference;
import edu.zju.gis.gncstatistic.Grid;
import edu.zju.gis.spark.ParallelTools.RangeCondition.DistrictRange;
import edu.zju.gis.spark.ParallelTools.RangeCondition.PolygonRange;
import edu.zju.gis.spark.ParallelTools.RangeCondition.RangeCondition;
import edu.zju.gis.spark.ParallelTools.RangeCondition.RangeConditionFactory;
import edu.zju.gis.spark.ParallelTools.TransformFunctions.LcraBufferMapFunction_Dist;
import edu.zju.gis.spark.ParallelTools.TransformFunctions.LcraBufferMapFunction_poly;
import edu.zju.gis.spark.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.gdal.gdal.gdal;
import org.gdal.ogr.ogr;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * Created by Zhy on 2017/11/1.
 * bufferdistance:缓冲区距离
 * outputformat:选择输出文件的格式
 * pertasknum:提交一次任务中的格网数量
 */
public class SpatialBuffer {

    public static void main(String[] args) throws IOException {
        gdal.AllRegister();
        ogr.RegisterAll();
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("SpatialBufferModel");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //读取模型参数
//        OperationsParams parameters = new OperationsParams(new GenericOptionsParser(args));
        Map parameters = Utils.ParseParameters(args);
        String applicationID = (String)parameters.get("applicationid");
        String ccFilter = (String)parameters.get("categorycode");
//        int perTaskNum = Integer.parseInt((String)parameters.get("pertasknum"));  //一次提交任务需要处理的格网数量
        int perTaskNum= 50;
        double bufferDist = Double.parseDouble((String)parameters.get("bufferdistance"));
        String outputFormat = (String)parameters.get("outputformat");
        String lcraGridPath = (String)parameters.get("lcragridpath");
        Map<String,Integer> ccFilterMap = Utils.ccFliterFormat(ccFilter);

        RangeCondition rangeCondition = RangeConditionFactory.createRangeCondition(parameters);
        JavaPairRDD<String, Iterable<Tuple2<String, Geometry>>> gridRDD = Utils.getExtention(rangeCondition,sc);
        gridRDD.cache();

        List<String> gridKeys = gridRDD.keys().distinct().sortBy(new Function<String, Object>() {
            @Override
            public Object call(String s) throws Exception {
                return s;
            }
        }, true, 1).collect();

        //将grid划分到一个或多个job
//        int perTaskNum = (slimGridKeys.size() / taskNum) + 1;
        int jobNum = gridKeys.size()/perTaskNum+1;
        Map<String, Integer> taskSchema = Utils.splitTask(perTaskNum,gridKeys);

        System.out.println(" ========== SLIM GRID KEY NUM ========== ");
        System.out.println(gridKeys.size());
        System.out.println(" ========== TASK SCHEMA NUM ========== ");
        System.out.println(taskSchema.size());

        //对gridRDD 赋予jobID值 用于循环提交job
        final Broadcast<Map<String, Integer>> taskSchemaBC = sc.broadcast(taskSchema);
        JavaPairRDD<String, Tuple2<String, Iterable<Tuple2<String, Geometry>>>> gridWithJobIdRDD = gridRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Tuple2<String, Geometry>>>, String, Tuple2<String, Iterable<Tuple2<String, Geometry>>>>() {
            @Override
            public Tuple2<String, Tuple2<String, Iterable<Tuple2<String, Geometry>>>> call(Tuple2<String, Iterable<Tuple2<String, Geometry>>> stringIterableTuple2) throws Exception {
                Tuple2<String, Tuple2<String, Iterable<Tuple2<String, Geometry>>>> out = new Tuple2<String, Tuple2<String, Iterable<Tuple2<String, Geometry>>>>(String.valueOf(taskSchemaBC.getValue().get(stringIterableTuple2._1())), stringIterableTuple2);
                return out;
            }
        });
        gridRDD.unpersist();
        gridWithJobIdRDD.cache();
        System.out.println("jobnum : "+jobNum);
        List<String> bufferResult_all = new ArrayList<String>();
        for (int i = 1; i <= jobNum; i++) {
            //如果直接按照省市县进行查询  则获取格网对应的lcra数据 根据regionCode进行过滤即可
            //如果按照多边形进行查询  则需要获取格网中对应的lcra数据  对其进行空间相交操作

            List<Tuple2<String, Iterable<Tuple2<String, Geometry>>>> gridInSingleJob = gridWithJobIdRDD.lookup(String.valueOf(i));
            System.out.println("gridWithJobIdRDD : "+gridInSingleJob.size() );
            List<Grid> grids = new ArrayList<Grid>();

            //grids：第i个task的细粒度格网列表
            for (Tuple2<String, Iterable<Tuple2<String, Geometry>>> cell : gridInSingleJob) {
                grids.add(Grid.Parse(cell._1()));
            }
            //lcraRDD：格网号+cc  geometry
            JavaRDD<String> subBuffer = null;
            JavaPairRDD<String, Iterable<String>> lcraRDD = Utils.getSpatialLcra(grids,ccFilterMap,sc,lcraGridPath);
            lcraRDD.cache();
            System.out.println("============lcraRDD===========");
            List<Tuple2<String,Iterable<String>>> lcraList = lcraRDD.collect();
            for(Tuple2<String,Iterable<String>> lcra:lcraList){
                System.out.println(lcra._1());
            }
            if(rangeCondition instanceof DistrictRange){
                subBuffer = lcraRDD.flatMap(new LcraBufferMapFunction_Dist((DistrictRange)rangeCondition,bufferDist));
            }
            else if(rangeCondition instanceof PolygonRange){
                 subBuffer = lcraRDD.flatMap(new LcraBufferMapFunction_poly(gridInSingleJob,bufferDist));
            }
            subBuffer.cache();
            System.out.println("subBuffer size:"+subBuffer.collect().size());
            bufferResult_all.addAll(subBuffer.collect());
        }
        System.out.println(bufferResult_all.size());
//        for(String buff:bufferResult_all){
//            System.out.println("=========result=========");
//            System.out.println(buff);
//        }

        //输出结果文件
        String modelName = "BUFFER";
        Utils.parallelResultOutput(sc.parallelize(bufferResult_all),outputFormat,modelName,applicationID);
        System.out.println("求解完成！");


    }
}
