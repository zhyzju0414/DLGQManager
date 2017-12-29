package edu.zju.gis.spark.SurfaceAreaCalculation;

import com.esri.core.geometry.Geometry;
import edu.zju.gis.spark.SurfaceAreaCalculation.CalculationCondition.CalculationCondition;
import edu.zju.gis.spark.SurfaceAreaCalculation.CalculationCondition.CalculationConditionFactory;
import edu.zju.gis.spark.SurfaceAreaCalculation.CalculationCondition.PropertyCalculationCondition;
import edu.zju.gis.spark.SurfaceAreaCalculation.CalculationCondition.SpatialCalculationCondition;
import edu.zju.gis.spark.Utils;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.gdal.gdal.gdal;
import org.gdal.ogr.DataSource;
import org.gdal.ogr.Layer;
import org.gdal.ogr.ogr;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Zhy on 2017/9/21.
 */
public class SparkMain {
    public static void main(String[] args) throws Exception {
        gdal.AllRegister();
        ogr.RegisterAll();
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("SurfaceAreaCalculation");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //读取要素数据
//        OperationsParams parameters = new OperationsParams(new GenericOptionsParser(args));
        Map parameters = Utils.ParseParameters(args);
        String lcraFilePath = (String) parameters.get("lcrafilepath");
        String demFilePath = (String) parameters.get("demfilepath");
        int perCircleNum = Integer.valueOf((String) parameters.get("percirclenum"));

        DataSource ds = ogr.Open("/SSD_home/zhy/uploadShp/Beilun.shp");
        Layer layer = ds.GetLayer(0);
        org.gdal.ogr.Geometry geo = layer.GetNextFeature().GetGeometryRef();
        String testWkt = geo.ExportToWkt();
        Map<String,String> conditionParameters = new HashMap<String,String>();
        conditionParameters.put("POLYGONLEVEL","1115");
        conditionParameters.put("REGIONCODE","330206");
        conditionParameters.put("CATEGORYCODE","0120");
        conditionParameters.put("GEOWKT",testWkt);
        CalculationCondition condition = CalculationConditionFactory.createCalculationCondition(conditionParameters);
        System.out.println(condition.toString());
        IFeatureCalcalation calcalationObj=null;
        if(condition instanceof PropertyCalculationCondition){
            calcalationObj = new PropertyCalculationClass();
        }
        else if(condition instanceof SpatialCalculationCondition){
            calcalationObj = new SpatialCalculationClass();
        }

        List<String> allSheetCodeList = IFeatureCalcalation.getSheetRangeByWkt(testWkt);
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
            //allLcraByCodeRDD格式：key:sheetcode  value:sheetcode \t rowkey \t wkt
            JavaPairRDD<String, Iterable<String>> allLcraByCodeRDD = calcalationObj.getFilteredRDD(condition,sheetCodes,lcraFilePath,sc);
            allLcraByCodeRDD.cache();
            List<Tuple2<String, Iterable<String>>> allLcraByCodeList = allLcraByCodeRDD.collect();
            for(Tuple2<String, Iterable<String>> tuple:allLcraByCodeList){
                System.out.print("sheetcode: "+tuple._1()+"\t");
            }
            System.out.println("========lcraRDD2============");
            System.out.println("lcraRDD2: "+allLcraByCodeList.size());
            Tuple2<String, Iterable<String>> tuple2 = allLcraByCodeList.get(0);
            System.out.println("key=" +tuple2._1()+"  value= "+tuple2._2());
            //lcraRDD2格式：key:sheetcode  value:regioncode+"\t"+ccCode+"\t"+wkt

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
