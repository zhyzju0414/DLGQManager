package edu.zju.gis.spark.SurfaceAreaCalculation;

import com.esri.core.geometry.Geometry;
import edu.zju.gis.gncstatistic.Sheet;
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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.gdal.gdal.gdal;
import org.gdal.ogr.ogr;
import org.omg.CosNaming.NamingContextExtPackage.StringNameHelper;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * Created by Zhy on 2017/11/17.
 */
public class SparkMain1117 {
    public static void main(String[] args) throws IOException {
        gdal.AllRegister();
        ogr.RegisterAll();
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("SurfaceAreaCalculation");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //读取要素数据
//        OperationsParams parameters = new OperationsParams(new GenericOptionsParser(args));
        Map parameters = Utils.ParseParameters(args);
        String applicationID = (String)parameters.get("applicationid");
        String lcraSheetPath = (String) parameters.get("lcrasheetpath");
        String demFilePath = (String) parameters.get("demfilepath");
//        int perTaskNum = Integer.valueOf((String) parameters.get("percirclenum"));
        int perTaskNum = 50;
        String ccFilter = (String)parameters.get("categorycode");
        Map<String,Integer> ccFilterMap = Utils.ccFliterFormat(ccFilter);

        RangeCondition rangeCondition = RangeConditionFactory.createRangeCondition(parameters);
        JavaPairRDD<String, Iterable<Tuple2<String, Geometry>>> sheetRDD = Utils.getSheetExtention(rangeCondition,sc);

        List<String> sheetKeys = sheetRDD.keys().distinct().sortBy(new Function<String, Object>() {
            @Override
            public Object call(String s) throws Exception {
                return s;
            }
        }, true, 1).collect();

        //将grid划分到一个或多个job
//        int perTaskNum = (slimGridKeys.size() / taskNum) + 1;
        int jobNum = sheetKeys.size()/perTaskNum+1;
        Map<String, Integer> taskSchema = Utils.splitTask(perTaskNum,sheetKeys);
        System.out.println(" ========== SLIM GRID KEY NUM ========== ");
        System.out.println(sheetKeys.size());
        System.out.println(" ========== TASK SCHEMA NUM ========== ");
        System.out.println(taskSchema.size());

        //对gridRDD 赋予jobID值 用于循环提交job
        final Broadcast<Map<String, Integer>> taskSchemaBC = sc.broadcast(taskSchema);
        JavaPairRDD<String, Tuple2<String, Iterable<Tuple2<String, Geometry>>>> sheetWithJobIdRDD = sheetRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Tuple2<String, Geometry>>>, String, Tuple2<String, Iterable<Tuple2<String, Geometry>>>>() {
            @Override
            public Tuple2<String, Tuple2<String, Iterable<Tuple2<String, Geometry>>>> call(Tuple2<String, Iterable<Tuple2<String, Geometry>>> stringIterableTuple2) throws Exception {
                Tuple2<String, Tuple2<String, Iterable<Tuple2<String, Geometry>>>> out = new Tuple2<String, Tuple2<String, Iterable<Tuple2<String, Geometry>>>>(String.valueOf(taskSchemaBC.getValue().get(stringIterableTuple2._1())), stringIterableTuple2);
                return out;
            }
        });
        sheetRDD.unpersist();
        sheetWithJobIdRDD.cache();
        System.out.println("jobnum : "+jobNum);

        Map<String, Double> finalResult = new HashMap<String, Double>();
        for (int c = 1; c <= jobNum; c++) {
            System.out.println("=========circlr "+c+" start==========");
            //获取这一次任务中所有的分幅号
            List<Tuple2<String, Iterable<Tuple2<String, Geometry>>>> sheetInSingleJob = sheetWithJobIdRDD.lookup(String.valueOf(c));
            System.out.println("sheetWithJobIdRDD : "+sheetInSingleJob.size() );
            List<Sheet> sheets = new ArrayList<Sheet>();

            //grids：第i个task的细粒度格网列表
            for (Tuple2<String, Iterable<Tuple2<String, Geometry>>> cell : sheetInSingleJob) {
                sheets.add(new Sheet(cell._1()));
            }
            //lcraRDD  在分幅中的lcra：分幅号#cc  rowkey \t geometry
            JavaPairRDD<String, Iterable<String>> lcraRDD = Utils.getSpatialLcra(sheets,ccFilterMap,sc,lcraSheetPath,1);
            JavaRDD<String> lcraInPolygonRDD = null;
            JavaPairRDD<String, Iterable<String>> lcraRDD2 =null;
            if(rangeCondition instanceof DistrictRange){
                //lcraInPolygonRDD格式：  SheetCode+"\t"+rowkey+"\t"+wkt
                lcraInPolygonRDD = lcraRDD.flatMap(new LcraClipMapFunction_Dist((DistrictRange)rangeCondition,"surfaceArea"));

            }
            else if(rangeCondition instanceof PolygonRange){
                lcraInPolygonRDD = lcraRDD.flatMap(new LcraClipMapFunction_Poly(sheetInSingleJob,"surfaceArea"));
            }
//            lcraInPolygonRDD.cache();
//            List<String> list2 = lcraInPolygonRDD.collect();
//            System.out.println("list2:"+list2.size());

            lcraRDD2 = lcraInPolygonRDD.mapToPair(new PairFunction<String, String, String>() {
                @Override
                public Tuple2<String, String> call(String s) throws Exception {
                    String[] values = s.split("\t");
                    String sheetcode = values[0];
                    String rowkey = values[1];
                    String wkt = values[2];
                    String value = sheetcode+"\t"+rowkey+"\t"+wkt;
                    return new Tuple2<String, String>(sheetcode,value);
                }
            }).groupByKey();
            lcraRDD2.cache();
            List<Tuple2<String, Iterable<String>>> lcraRDD2List = lcraRDD2.collect();
            System.out.println("========lcraRDD2============");
            System.out.println("lcraRDD2: "+lcraRDD2List.size());
            Tuple2<String, Iterable<String>> tuple2 = lcraRDD2List.get(0);
            System.out.println("key=" +tuple2._1()+"  value= "+tuple2._2());
            //lcraRDD2格式：key:sheetcode  value:regioncode+"\t"+ccCode+"\t"+wkt
            JavaPairRDD<String, Double> resultRDD = lcraRDD2.flatMapToPair(new SurfaceAreaPairFunciton_modify(demFilePath)).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double aDouble, Double aDouble2) throws Exception {
                    return aDouble + aDouble2;
                }
            });
            //这里的key值为 “regionCode#CC”

            Map<String, Double> resultList = resultRDD.collectAsMap();
            Set<String> codes = resultList.keySet();
            for (String code : codes) {
//                System.out.println(code+","+cellResult.get(code));
                if (finalResult.containsKey(code)) {
                    double area = Double.valueOf(finalResult.get(code));
                    double cellarea = Double.valueOf(resultList.get(code));
                    finalResult.put(code, (area + cellarea));
                } else {
                    finalResult.put(code, resultList.get(code));
                }
            }
        }

        //最后的结果是result  输出result
        //这里的key值为 “regionCode#CC”
        List<Map<String, String>> resultList = new ArrayList<Map<String, String>>();
        Iterator it = finalResult.entrySet().iterator();
        double areaSum = 0;
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            String regionCode = entry.getKey().toString().split("#")[0];
            String CC = entry.getKey().toString().split("#")[1];
            double value = (Double) entry.getValue();
            System.out.println("行政区：" + regionCode + " 中 " + CC + " 类的的地表面积为 " + value);
            Map<String, String> map = new HashMap<String, String>();
            map.put("REGIONCODE", regionCode);
            map.put("CCCODE", CC);
            map.put("SURFACEAREA", String.valueOf(value));
            resultList.add(map);
            areaSum+=value;
        }
        System.out.println("总面积为："+areaSum);
        Utils.SaveResultInOracle(applicationID, resultList);
    }

}
