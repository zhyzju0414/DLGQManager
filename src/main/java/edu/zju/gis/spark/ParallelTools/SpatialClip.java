package edu.zju.gis.spark.ParallelTools;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.SpatialReference;
import edu.zju.gis.gncstatistic.Grid;
import edu.zju.gis.spark.ParallelTools.RangeCondition.DistrictRange;
import edu.zju.gis.spark.ParallelTools.RangeCondition.PolygonRange;
import edu.zju.gis.spark.ParallelTools.RangeCondition.RangeCondition;
import edu.zju.gis.spark.ParallelTools.RangeCondition.RangeConditionFactory;
import edu.zju.gis.spark.ParallelTools.TransformFunctions.LcraClipMapFunction_Dist;
import edu.zju.gis.spark.ParallelTools.TransformFunctions.LcraClipMapFunction_Poly;
import edu.zju.gis.spark.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.gdal.gdal.gdal;
import org.gdal.ogr.ogr;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Created by Zhy on 2017/11/5.
 */
public class SpatialClip {
    public static void main(String[] args) throws IOException {
        gdal.AllRegister();
        ogr.RegisterAll();
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("SpatialClipModel");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //读取模型参数
        Map parameters = Utils.ParseParameters(args);
        String applicationID = (String)parameters.get("applicationid");
        String ccFilter = (String)parameters.get("categorycode");
//        int perTaskNum = Integer.parseInt((String)parameters.get("pertasknum"));  //一次提交任务需要处理的格网数量
        int perTaskNum= 50;
//        double bufferDist = Double.parseDouble((String)parameters.get("bufferdistance"));
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
        List<String> clipResult_all = new ArrayList<String>();

        for (int i = 1; i <= jobNum; i++) {
            List<Tuple2<String, Iterable<Tuple2<String, Geometry>>>> gridInSingleJob = gridWithJobIdRDD.lookup(String.valueOf(i));
            System.out.println("gridWithJobIdRDD : "+gridInSingleJob.size() );
            List<Grid> grids = new ArrayList<Grid>();

            //grids：第i个task的细粒度格网列表
            for (Tuple2<String, Iterable<Tuple2<String, Geometry>>> cell : gridInSingleJob) {
                grids.add(Grid.Parse(cell._1()));
            }
            //lcraRDD：格网号+cc  geometry
            JavaPairRDD<String, Iterable<String>> lcraRDD = Utils.getSpatialLcra(grids,ccFilterMap,sc,lcraGridPath);
            JavaRDD<String> subClip = null;
            if(rangeCondition instanceof DistrictRange){
                subClip = lcraRDD.flatMap(new LcraClipMapFunction_Dist((DistrictRange)rangeCondition,"normal"));
            }
            else if(rangeCondition instanceof PolygonRange){
                subClip = lcraRDD.flatMap(new LcraClipMapFunction_Poly(gridInSingleJob,"normal"));
            }
            subClip.cache();
            System.out.println("subBuffer size:"+subClip.collect().size());
            clipResult_all.addAll(subClip.collect());
        }
        System.out.println(clipResult_all.size());
//        for(String buff:clipResult_all){
//            System.out.println("=========result=========");
//            System.out.println(buff);
//        }

        //输出结果文件
        String modelName = "CLIP";
        Utils.parallelResultOutput(sc.parallelize(clipResult_all),outputFormat,modelName,applicationID);
        System.out.println("求解完成！");
    }
}
