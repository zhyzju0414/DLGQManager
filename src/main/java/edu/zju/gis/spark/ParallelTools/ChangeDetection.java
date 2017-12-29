package edu.zju.gis.spark.ParallelTools;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.SpatialReference;
import edu.zju.gis.gncstatistic.Grid;
import edu.zju.gis.spark.ParallelTools.RangeCondition.RangeCondition;
import edu.zju.gis.spark.ParallelTools.RangeCondition.RangeConditionFactory;
import edu.zju.gis.spark.ParallelTools.TransformFunctions.ChangeDetectMapFunction;
import edu.zju.gis.spark.ParallelTools.TransformFunctions.LcraClipMapFunction_Poly;
import edu.zju.gis.spark.ParallelTools.TransformFunctions.LcraClipMapFunction_Poly2;
import edu.zju.gis.spark.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.deploy.rest.SubmissionStatusResponse;
import org.gdal.gdal.gdal;
import org.gdal.ogr.DataSource;
import org.gdal.ogr.ogr;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;


/**
 * Created by Zhy on 2017/11/5.
 * 两年地表覆盖要素的变化提取
 * 输入参数：
 * regioncode：用户输入的省市县行政编码
 * ccFliter：仅看这一类地物的变化情况  如果为空 则看所有地表覆盖类型的变化情况
 */
public class ChangeDetection {

    public static void main(String[] args) throws IOException {
        gdal.AllRegister();
        ogr.RegisterAll();
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("ChangeDetectionModel");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //读取模型要素
        Map parameters = Utils.ParseParameters(args);
        String applicationID = (String)parameters.get("applicationid");
        String ccFilter = (String)parameters.get("categorycode");
//        int perTaskNum = Integer.parseInt((String)parameters.get("pertasknum"));  //一次提交任务需要处理的格网数量
        int perTaskNum = 50;
        String lcraGridPath_year1 = (String)parameters.get("dataset1");
        String lcraGridPath_year2=(String)parameters.get("dataset2");
        String outputFormat = (String)parameters.get("outputformat");
        Map<String,Integer> ccFilterMap = Utils.ccFliterFormat(ccFilter);

        //获取查询范围的多边形所在的格网  并进行切割
        RangeCondition rangeCondition = RangeConditionFactory.createRangeCondition(parameters);
        JavaPairRDD<String, Iterable<Tuple2<String, Geometry>>> gridRDD = Utils.getExtention(rangeCondition,sc);
        gridRDD.cache();

        //对格网进行分批处理
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
        System.out.println("jobNum = "+jobNum);
        //存放最后的变化检测结果
        List<String> changeDetectResult_all = new ArrayList<String>();
        for(int i=1;i<=jobNum;i++){
            System.out.println("===========开始进行叠加============");
            Date start = new Date();
            //将格网转化成grid类型的格网列表
            List<Tuple2<String, Iterable<Tuple2<String, Geometry>>>> gridInSingleJob = gridWithJobIdRDD.lookup(String.valueOf(i));
            System.out.println("gridWithJobIdRDD : "+gridInSingleJob.size() );
            List<Grid> grids = new ArrayList<Grid>();

            //grids：第i个task的细粒度格网列表
            for (Tuple2<String, Iterable<Tuple2<String, Geometry>>> cell : gridInSingleJob) {
                grids.add(Grid.Parse(cell._1()));
            }
            //分别获取这些格网下的两年的lcra要素数据  lcraRDD1，lcraRDD2
            //格网号+cc码    rowkey Geometry
            //如果lastyearbased=1  提取lcraRDD1中ccFilter类型的要素（小）  提取lcraRDD2中非ccFilter类型的要素（大）
            JavaPairRDD<String, Iterable<String>> lcraRDD1 = Utils.getSpatialLcra(grids,ccFilterMap,sc,lcraGridPath_year1);
            //lcraRDD2格式：key:gridcode#cc  value:rowkey+"\t"+wkt
            Map<String,Integer> ccMap = new HashMap<String,Integer>();
            JavaPairRDD<String,Iterable<String>> lcraRDD2 = Utils.getSpatialLcra(grids,ccMap,sc,lcraGridPath_year2).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
                @Override
                public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
                    String key = stringIterableTuple2._1();
                    String gridCode = key.split("#")[0];
                    String ccCode = key.split("#")[1];
                    List<Tuple2<String,String>> outputValue = new ArrayList<Tuple2<String,String>>();
                    Iterator<String> valueIterator = stringIterableTuple2._2().iterator();
                    while (valueIterator.hasNext()){
                        String s = valueIterator.next();
                        String regionCode = s.split("\t")[0].substring(0,6);
                        String wkt = s.split("\t")[1];
                        String outputStr = regionCode+"\t"+ccCode+"\t"+wkt;
                        outputValue.add(new Tuple2<String,String>(gridCode,outputStr));
                    }
                    return outputValue.iterator();
                }
            }).groupByKey();


            //获取lcraRDD中真正与多边形相交的部分
            //lcraRDD1格式：key:gridCode  value:regioncode+"\t"+ccCode+"\t"+intersectWkt
            //lcraRDD2格式: key:gridCode  value:regioncode+"\t"+ccCode+"\t"+intersectWkt
            JavaPairRDD<String,Iterable<String>> lcra1InCountyRDD = lcraRDD1.flatMap(new LcraClipMapFunction_Poly2(gridInSingleJob)).mapToPair(new PairFunction<String, String, String>() {
                @Override
                public Tuple2<String, String> call(String s) throws Exception {
                    String[] values = s.split("\t");
                    String gridCode = values[0];
                    String value =null;
                    for(int i=1;i<4;i++){
                        value+=(values[i]+"\t");
                    }
                    return new Tuple2<String, String>(gridCode,value.toString());
                }
            }).groupByKey();
            JavaPairRDD<String,Tuple2<Iterable<String>,Iterable<String>>> joinRDD = lcra1InCountyRDD.join(lcraRDD2);
            joinRDD.cache();
            List<Tuple2<String,Tuple2<Iterable<String>,Iterable<String>>>> joinList = joinRDD.collect();
            System.out.println("joinList:"+joinList.size());
            for(Tuple2<String,Tuple2<Iterable<String>,Iterable<String>>> join:joinList){
                System.out.println(join._1());
            }
            Date joinTime = new Date();
            System.out.println("完成join！");
            System.out.println("到join所需时间为： " + (joinTime.getTime() - start.getTime()) / 1000.00 + " 秒");
            //提取lcra1和lcra2中不同类型的部分 即两个RDD相交的部分
            //format:格网号  iterable<Geometry>
            JavaRDD<String> subChangedLcraRDD = joinRDD.flatMap(new ChangeDetectMapFunction());
            changeDetectResult_all.addAll(subChangedLcraRDD.collect());
            System.out.println(changeDetectResult_all.size());
            Date end = new Date();
            System.out.println("求解完成！");
            System.out.println("总共所需时间为： " + (end.getTime() - start.getTime()) / 1000.00 + " 秒");
        }

        //输出结果：
        String modelName = "CHANGE_DETECT";
        Utils.parallelResultOutput(sc.parallelize(changeDetectResult_all),outputFormat,modelName,applicationID);
        System.out.println("求解完成！");
    }


}
