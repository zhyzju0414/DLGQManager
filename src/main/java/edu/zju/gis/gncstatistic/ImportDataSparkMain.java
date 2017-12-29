package edu.zju.gis.gncstatistic;

import com.esri.core.geometry.Geometry;
import edu.zju.gis.spark.SurfaceAreaCalculation.SaveDataInSheetPairFunction;
import edu.zju.gis.spark.SurfaceAreaCalculation.SheetPartitionPairFlatMapFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 将数据按照格网进行组织，每一个格网内构建空间索引，将WKT与属性信息进行分离
 * args[0] 源数据组织目录
 * args[1] 目标数据根目录
 * args[2] 选取的格网框架
 * args[3] gridsize
 * args[4] hdfs root
 * Created by Frank on 2017/6/15.
 */
public class ImportDataSparkMain {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf();
        conf.setAppName("ImportDataSparkMain");
        JavaSparkContext ctx = new JavaSparkContext();
        List<Tuple2<String, String>> resultLog =null;

        if(args[2].equals("latlon")) {
            resultLog = ctx.textFile(args[0]).flatMapToPair(new GridPartitionPairFlatMapFunction(Double.parseDouble(args[3]), args[2])).
                            groupByKey().mapToPair(new SaveDataInGridPairFunction(args[1], args[4], Double.parseDouble(args[3]), args[2])).collect();
        }
        else if(args[2].equals("sheet")){
            resultLog = ctx.textFile(args[0],2777).flatMapToPair(new SheetPartitionPairFlatMapFunction(Integer.parseInt(args[3]),args[2])).
                    groupByKey().mapToPair(new SaveDataInSheetPairFunction(args[1],args[4], Integer.parseInt(args[3]),args[2])).collect();
//            JavaPairRDD<String, Tuple2<Geometry, String>> tmp = ctx.textFile(args[0]).flatMapToPair(new SheetPartitionPairFlatMapFunction(Integer.parseInt(args[3]),args[2]));
//            Map<String, Tuple2<Geometry, String>> tmpMap = tmp.collectAsMap();
//            System.out.println(tmpMap.size());
        }
        //写入日志
        int count = 0;
        int error = 0;
        List<String> wktlist = new ArrayList<String>();
        for(int i=0;i<resultLog.size();i++){
            if(resultLog.get(i)._2.startsWith("fail")){
                System.out.println(resultLog.get(i)._1+"\t"+resultLog.get(i)._2);
                error++;
            }else{
//                Grid grid = Grid.Parse(resultLog.get(i)._1);
//                Envelope envelope = GridArchitectureFactory.GetGridArchitecture(Double.parseDouble(args[3]),args[2]).GetSpatialRange(grid);
//                System.out.println(grid.Col+","+grid.Row);
//                wktlist.add(GeometryEngine.geometryToWkt(envelope,0));
                count+=Integer.parseInt(resultLog.get(i)._2);
            }
        }

        System.out.println("总记录数："+count+";错误格网数："+error);

        for(int i=0;i<wktlist.size();i++){
            System.out.println(wktlist.get(i));
        }

    }
}
