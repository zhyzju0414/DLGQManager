package edu.zju.gis.spark.SurfaceAreaCalculation;

import edu.zju.gis.spark.TransformationTool.OperationsParams;
import edu.zju.gis.spark.Utils;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * Created by dlgq on 2017/8/20.
 */
public class SurfaceAreaCalculation_modify {
    public static void main(String[] args) throws IOException {
        gdal.AllRegister();
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("SurfaceAreaCalculation");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //读取要素数据
//        OperationsParams parameters = new OperationsParams(new GenericOptionsParser(args));
        Map parameters = Utils.ParseParameters(args);
        String lcraFilePath = (String) parameters.get("lcrafilepath");
        String demFilePath = (String) parameters.get("demfilepath");
        int perCircleNum = Integer.valueOf((String) parameters.get("percirclenum"));
//        final Broadcast<String> demFilePathBC = sc.broadcast(demFilePath);
        //获取这个路径下面所有的分幅编号
        List<String> allSheetCodeList = null;
        try {
            allSheetCodeList = Utils.GetAllSheetCode(lcraFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int allSheetCodeCount = allSheetCodeList.size();
        System.out.println("一共有" + allSheetCodeCount + " 个分幅");
        //确定每一次提交任务要执行多少个分幅
        int circleNum;
        if (allSheetCodeCount % perCircleNum == 0) {
            circleNum = allSheetCodeCount / perCircleNum;
        } else {
            circleNum = (allSheetCodeCount / perCircleNum) + 1;    //循环提交的次数
        }
        List<Tuple2<Integer, String>> sheetCodeSchema = new ArrayList<Tuple2<Integer, String>>();
        int num = 0;
        int circle = 0;
        for (int i = 0; i < allSheetCodeCount; i++) {
            num++;
            if (num <= perCircleNum) {
                sheetCodeSchema.add(new Tuple2<Integer, String>(circle, allSheetCodeList.get(i)));
            } else if (circle < circleNum) {
                circle++;
                sheetCodeSchema.add(new Tuple2<Integer, String>(circle, allSheetCodeList.get(i)));
                num = 1;
            } else {
                // 处理最后一个分片可能会多出来的个数
                sheetCodeSchema.add(new Tuple2<Integer, String>(circle, allSheetCodeList.get(i)));
            }
        }
        System.out.println(sheetCodeSchema.size());
        for (Tuple2<Integer, String> l : sheetCodeSchema) {
            System.out.println(l._1() + " " + l._2());
        }
        JavaPairRDD<Integer, String> allSheetCodeRDD = sc.parallelizePairs(sheetCodeSchema);

        //result用来存放最后的结果
        Map<String, Double> result = new HashMap<String, Double>();
        allSheetCodeRDD.cache();

        for (int c = 0; c < circleNum; c++) {
            System.out.println("=========circlr "+c+" start==========");
            //获取这一次任务中所有的分幅号
            final List<String> sheetCodes = allSheetCodeRDD.lookup(c);
            String lcraPath = lcraFilePath + "/" +sheetCodes.get(0).substring(0,3)+"/"+ sheetCodes.get(0) + "/wkt";
            JavaPairRDD<String, String> allLcraRDD = sc.textFile(lcraPath).mapToPair(new PairFunction<String, String, String>() {
                @Override
                public Tuple2<String, String> call(String s) throws Exception {
                    String[] values = s.split("\t");
                    String code = values[0];
                    Tuple2<String, String> res = new Tuple2<String, String>(code, s);
                    return res;
                }
            });
            for (int i = 1; i < sheetCodes.size(); i++) {
                //读取这个code下面的所有lcra要素
                lcraPath = lcraFilePath + "/" +sheetCodes.get(i).substring(0,3)+"/"+ sheetCodes.get(i) + "/wkt";
                JavaPairRDD<String, String> lcraRDD = sc.textFile(lcraPath).mapToPair(new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] values = s.split("\t");
                        String code = values[0];
                        Tuple2<String, String> res = new Tuple2<String, String>(code, s);
                        return res;
                    }
                });
                //用union将所有的RDD合并成一个RDD  假设叫temp1
                allLcraRDD = allLcraRDD.union(lcraRDD);
            }

//            allLcraRDD.cache();
            //将这些分幅下面所有的lcra要素按照格网组织
//            Set<String> keys = allLcraRDD.collectAsMap().keySet();
//            int sheetCodeCount = keys.size();
//            for (String key : keys) {
//                System.out.println("key = " + key);
//            }
            JavaPairRDD<String, Iterable<String>> allLcraByCodeRDD = allLcraRDD.groupByKey(sheetCodes.size());

            JavaPairRDD<String, Double> resultRDD = allLcraByCodeRDD.flatMapToPair(new SurfaceAreaPairFunciton_modify(demFilePath)).reduceByKey(new Function2<Double, Double, Double>() {
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
                if (result.containsKey(code)) {
                    double area = Double.valueOf(result.get(code));
                    double cellarea = Double.valueOf(resultList.get(code));
                    result.put(code, (area + cellarea));
                } else {
                    result.put(code, resultList.get(code));
                }
            }
        }

        //最后的结果是result  输出result
        //这里的key值为 “regionCode#CC”
        List<Map<String, String>> resultList = new ArrayList<Map<String, String>>();
        Iterator it = result.entrySet().iterator();
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

        String applicationID = (String) parameters.get("applicationid");
        Utils.SaveResultInOracle(applicationID, resultList);
    }
}
