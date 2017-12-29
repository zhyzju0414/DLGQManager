package edu.zju.gis.spark.SurfaceAreaCalculation;

import edu.zju.gis.spark.Utils;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import scala.Tuple2;

import java.lang.String;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static edu.zju.gis.spark.SurfaceAreaCalculation.DEMGrid.scanLineByBuf;

/**
 * Created by dlgq on 2017/8/14.
 */
public class SurfaceAreaPairFunciton_modify implements PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, Double> {

    java.lang.String demFilePath;

    public SurfaceAreaPairFunciton_modify(java.lang.String demFilePath) {
        this.demFilePath = demFilePath;
        gdal.AllRegister();
        System.out.println("class has been initialed");
    }


    @Override
    public Iterator<Tuple2<String, Double>> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
        gdal.AllRegister();
        List<Tuple2<String, Double>> res = new ArrayList<Tuple2<String, Double>>();
        String sheetCode = stringIterableTuple2._1().split("#")[0];
//        String sheetCode = stringIterableTuple2._1();
        Iterable<String> iterable = stringIterableTuple2._2();
        //根据分幅号找dem数据
        String demPath = Utils.GetDemPathBySheetCode(sheetCode, demFilePath);
        System.out.println("sheetCode = " + sheetCode);
        System.out.println("demPath = " + demPath);
        Dataset hds = null;
        double[] geoTrans = null;
        Band band=null;
        try {
            hds = gdal.Open(demPath);
            geoTrans = hds.GetGeoTransform();
        } catch (Exception e) {
            System.out.println("找不到对应的dem数据！sheetcode= " + sheetCode);
            return res.iterator();
        }
        band = hds.GetRasterBand(1);
        int gridXCount = hds.getRasterXSize();    //列数
        int gridYCount = hds.getRasterYSize();    //行数
        System.out.println(sheetCode + " dem数据有" + gridYCount + "行");
        System.out.println(sheetCode + " dem数据有" + gridXCount + "列");
        float[] buf = new float[gridXCount * gridYCount];
        band.ReadRaster(0, 0, gridXCount, gridYCount, buf);
        for (int i = 0; i < buf.length; i++) {
            if (buf[i] > 10000 || buf[i] < -10000) {
                buf[i] = 0;
            }
        }
        //遍历这个分幅下所有的要素
        for (String s : iterable) {
            String[] values = s.split("\t");
            String wkt = values[2];
            String regionCode = values[1].substring(0, 6);
            String featureID = values[1].substring(19, 24);
            String CC = values[1].substring(12, 16);
            org.gdal.ogr.Geometry geometry = org.gdal.ogr.Geometry.CreateFromWkt(wkt);
            ClipFeature clipFeature = new ClipFeature(geometry, featureID);
            double area;
            area = scanLineByBuf(geoTrans, clipFeature, gridXCount, gridYCount, buf);
            Tuple2<String, Double> areaTuple = new Tuple2<String, Double>(regionCode + "#" + CC, area);
            res.add(areaTuple);
        }
        return res.iterator();
    }


}
