package edu.zju.gis.spark.SurfaceAreaCalculation;

import edu.zju.gis.spark.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import scala.Tuple2;

import java.util.List;

import static edu.zju.gis.spark.SurfaceAreaCalculation.DEMGrid.scanLineByBuf;


/**
 * Created by zhy on 2017/8/9.
 */
public class SurfaceAreaCalculationSingleSheet {
    //啦啦啦啦啦啦   试一下~~~
    //计算思路：
    //暂时以六个10m分幅（30'30'）为一个单元（暂时称为要素单元） 将地表覆盖数据切成若干个要素单元 经过计算：假设全国有4000个要素单元
    //每个这样的单元的地表要素数据都放在一个wkt里面
    //首先 要确定要将这4000个要素单元分成多少个job  现在假设分成200个job  那么一次要处理的要素单元就有20个
    //将这20个要素单元里的数据读进来，读进来的结果应该是20个地表要素的RDD
    //将这20个RDD合并成一个大的RDD （lcraRDD） 作为一个job的原始数据  注意：在合并之前要在RDD里面标识出这个要素属于哪个图幅号
    //现在对于一个job：
    //根据一定的规则把这20个要素单元的DEM数据都读进来  6*20  应该会有120份DEM数据（每个分幅对应一个DEM文件）  存放到一个巨大的数组里面（内存应该是够的）
    //把这个巨大的数据broadcast到每个节点上
    //对lcraRDD的每条数据进行scanLine操作 计算每个要素的面积
    public static void main(String[] args) {
        gdal.AllRegister();
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("SurfaceAreaCalculationSingleSheet");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        //读取要素数据
        String lcraPath = args[0];
        JavaRDD<String> temp1 = sc.textFile(lcraPath,200);
        //读取dem数据 存放到数组
//        String demPath = "/zhy/testdata/demtest.tif";
        String demPath = args[1];
        Dataset hds = gdal.Open(demPath);
        double[] geoTrans = hds.GetGeoTransform();
        Band band = hds.GetRasterBand(1);
        int gridXCount = hds.getRasterXSize();    //列数
        int gridYCount = hds.getRasterYSize();    //行数
        System.out.println("该dem数据有"+gridYCount+"行");
        System.out.println("该dem数据有"+gridXCount+"列");
        final float[] buf = new float[gridXCount * gridYCount];
        band.ReadRaster(0, 0, gridXCount, gridYCount, buf);
        for(int i=0;i<buf.length;i++){
            if(buf[i]>10000||buf[i]<-10000){
                buf[i]=0;
            }
        }
        //将dem数据广播到每个节点上
        final Broadcast<float[]> demDataBC = sc.broadcast(buf);
        final Broadcast<double[]> geoTransBC = sc.broadcast(geoTrans);
        final Broadcast<Integer> gridXCountBC = sc.broadcast(gridXCount);
        final Broadcast<Integer> gridYCountBC = sc.broadcast(gridYCount);

        //假设现在只有一个job
        //最后生成每一个县级行政区的地表面积
        JavaPairRDD<String, Double> resultRDD = temp1.mapToPair(new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String s) throws Exception {
                double[] geoTrans = geoTransBC.getValue();
                int gridXCount = gridXCountBC.getValue();
                int gridYCount = gridYCountBC.getValue();
                float[] demData = demDataBC.getValue();
                String[] value = s.split("\t");
//                if(value.length!=13)
//                    return null;
                String regionCode = value[1].substring(0, 6);   //获取县级行政区代码
                String featureID = value[1].substring(19,24);
//                System.out.println("=========该要素的regionCode为："+regionCode+"==========");
                String wkt = value[2];
//                System.out.println("=========该要素的wkt为："+wkt+"==========");
                org.gdal.ogr.Geometry geometry = org.gdal.ogr.Geometry.CreateFromWkt(wkt);
                ClipFeature clipFeature = new ClipFeature(geometry,featureID);
                double area;
                area = scanLineByBuf(geoTrans, clipFeature, gridXCount, gridYCount, demData);
                Tuple2<String, Double> areaTuple = new Tuple2<String, Double>(regionCode, area);
                return areaTuple;
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        });

        //输出结果：
        List<Tuple2<String, Double>> resultList = resultRDD.collect();
        for (Tuple2<String, Double> result : resultList) {
            System.out.println(result._1() + "  " + result._2());
        }
    }
}
