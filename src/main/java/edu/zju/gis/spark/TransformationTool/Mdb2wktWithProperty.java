package edu.zju.gis.spark.TransformationTool;

import com.esri.core.geometry.*;
import org.gdal.gdal.gdal;
import org.gdal.ogr.*;
import org.gdal.ogr.Geometry;
import org.gdal.osr.CoordinateTransformation;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static edu.zju.gis.hbase.tool.LoadDataBySpark.getCentralMeridian;

/**
 * Created by zhy on 2017/6/24.
 * 将地理国情要素转成wkt 其中属性值需要带上字段名 并且需要获取多个图层
 * 用于存储进HBASE
 */
public class Mdb2wktWithProperty {
//    public final static String WGS84_proj = "PROJCS[\"WGS_1984_Web_Mercator\",GEOGCS[\"GCS_WGS_1984_Major_Auxiliary_Sphere\",DATUM[\"D_WGS_1984_Major_Auxiliary_Sphere\",SPHEROID[\"WGS_1984_Major_Auxiliary_Sphere\",6378137.0,0.0]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Mercator\"],PARAMETER[\"false_easting\",0.0],PARAMETER[\"false_northing\",0.0],PARAMETER[\"central_meridian\",0.0],PARAMETER[\"standard_parallel_1\",0.0],UNIT[\"Meter\",1.0],AUTHORITY[\"EPSG\",3785]]";
    public final static String source = "GEOGCS[\"GCS_China_Geodetic_Coordinate_System_2000\",DATUM[\"D_China_2000\",SPHEROID[\"CGCS2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433],AUTHORITY[\"EPSG\",4490]]";
    public final static String target = "";
    CoordinateTransformation coordinateTransformation;

    public Mdb2wktWithProperty() {
        ogr.RegisterAll();
        gdal.AllRegister();
        gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "NO");
        gdal.SetConfigOption("SHAPE_ENCODING", "GB2312");
//        org.gdal.osr.SpatialReference sourceProj = new org.gdal.osr.SpatialReference();
//        org.gdal.osr.SpatialReference targetProj = new org.gdal.osr.SpatialReference();
//        java.util.Vector vector1 = new java.util.Vector();
//        vector1.add(source);
//        sourceProj.ImportFromESRI(vector1);
//        java.util.Vector vector2 = new java.util.Vector();
//        vector2.add(WGS84_proj);
//        targetProj.ImportFromESRI(vector2);
//        this.coordinateTransformation = new CoordinateTransformation(sourceProj, targetProj);
    }

    /**
     * mdb转wkt
     *
     * @param mdbFilePath
     * @param fileOutputStream
     */
    public String mdb2wkt(String mdbFilePath, DataOutputStream fileOutputStream) {

        BufferedOutputStream bufferedInputStream = new BufferedOutputStream(fileOutputStream);
        StringBuffer stringBuffer = new StringBuffer();
        int flushCount = 0;
        int flushToralence = 150;


        try {
            DataSource ds = ogr.Open(mdbFilePath, false);
            if (ds == null) {
                return "ERROR" + "open [" + mdbFilePath + "] failed";
            }

            List<Layer> layerList = getAllLayers(ds);

            for (int i = 0; i < layerList.size(); i++) {
                int featCount = 0;
                Layer player = ds.GetLayer(i);
                String layerName = player.GetName();
                if(!(layerName.equals("V_BOUA42016")||layerName.equals("V_BOUA52016"))){
                    continue;
                }

                if (player != null) {
                    //判断是否是点图层  如果是的话就直接跳过
                    if (player.GetGeomType() == 1) {
                        continue;
                    }
                    FeatureDefn ftd = player.GetLayerDefn();
                    Feature feature = null;
                    String regionCode;
                    String DLBM;
                    String PAC;

                    while ((feature = player.GetNextFeature()) != null) {
                        featCount++;
                        flushCount++;

                        regionCode = feature.GetFieldAsString("AREACODE");   //12位长度   导出乡镇文件 以及高程文件用到的是PAC
                        DLBM = feature.GetFieldAsString("CC");    //导出lcra文件和乡镇文件用到的是cc码
                        PAC = feature.GetFieldAsString("PAC").substring(0,6);

//                    if (regionCode.substring(0, 2).equals("51")| regionCode.substring(0, 2).equals("65")) {     //导出乡镇区划时
                        //地类图斑数据暂时没有行政区代码
                        if (regionCode == null)
                            regionCode = "999999999";
                        else
                            regionCode = fillStirng(regionCode, 9, false);
                        if (DLBM == null)
                            DLBM = "999";

                        String caption = replaceBlank(feature.GetFieldAsString("NAME"));

                        String Property = "OBJECTID:" + featCount + "\t" + "CC:" + DLBM + "\t" + "NAME:" + caption + "\t" + "PAC:" + PAC + "\t";
                        stringBuffer.append(replaceBlank(Property));
                        //几何值
                        StringBuffer geometryWkt = new StringBuffer();
                        //geo1的坐标是CGCS2000经纬度
                        Geometry geo1 = feature.GetGeometryRef();
                        geometryWkt.append(geo1.ExportToWkt());
                        String geoTypeStr = getGeoType(geometryWkt);
                        int centralMeridian = getCentralMeridian(geo1.Centroid().GetX());
                        CoordinateTransformation ct = edu.zju.gis.gncstatistic.Utils.ProjTransform(source, edu.zju.gis.spark.Utils.targetProjMap.get(centralMeridian));
                        geo1.Transform(ct);
                        com.esri.core.geometry.Geometry geo2 = GeometryEngine.geometryFromWkt(geo1.ExportToWkt(), WktImportFlags.wktImportDefaults, com.esri.core.geometry.Geometry.Type.Unknown);
                        double length = geo2.calculateLength2D();
                        stringBuffer.append(length + "\t");
                        if (geoTypeStr.equals("POLYGON") || geoTypeStr.equals("MULTIPOLYGON")) {
                            //计算面积
                            double area = geo1.Area();
                            stringBuffer.append(area + "\t");
                        } else {//如果是线要素的话就输出0
                            stringBuffer.append("0" + "\t");
                        }
                        stringBuffer.append(featCount + "\t");
                        stringBuffer.append(regionCode + "\t");
                        stringBuffer.append(caption + "\t");
                        stringBuffer.append(DLBM + "\t");
                        stringBuffer.append(geoTypeStr + "\t");
                        stringBuffer.append(geometryWkt);
                        if (flushCount == flushToralence) {
                            //输出
//                            System.out.println("flush on " + featCount);
                            bufferedInputStream.write(String.valueOf(stringBuffer).getBytes());
                            bufferedInputStream.flush();
                            flushCount = 0;
                            stringBuffer = new StringBuffer();
                        }
                        stringBuffer.append("\r\n");
                    }

                    //剩余输出
                    bufferedInputStream.write(String.valueOf(stringBuffer).getBytes());
                    bufferedInputStream.flush();
                    flushCount = 0;
                    stringBuffer = new StringBuffer();
                    System.out.println("已成功导入图层："+layerName);
                } else {
                    System.out.println("layer为空" + layerName);
                }
            }
            bufferedInputStream.close();
        } catch (Exception e) {
            String errorMessage = "ERROR:mdbPath=" + mdbFilePath + ",message=" + e.getMessage() + "  featurecount=" + "  errror:" + stackTraceToString(e);
            return errorMessage;
        }
        return "SUCCESS";
    }

    public static String stackTraceToString(Exception e) {
        StringBuffer sb = new StringBuffer();
        StackTraceElement[] stackTraceElements = e.getStackTrace();
        for (int i = 0; i < stackTraceElements.length; i++) {
            sb.append(stackTraceElements[i].toString() + "\n");
        }
        return sb.toString();
    }

    public static String replaceBlank(String str) {
        String dest = "";
        if (str != null) {
            Pattern p = Pattern.compile("(\r\n|\r|\n|\n\r)");
            Matcher m = p.matcher(str);
            dest = m.replaceAll("");
        }
        return dest;
    }

    public static String fillStirng(String fid_str, int totalLength, boolean isFrontOrNot) {
//        String fid_str = String.valueOf(FID);
        int str_len = fid_str.length();
        char[] chars = new char[totalLength - str_len];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = '0';
        }
        String str;
        if (isFrontOrNot) {
            str = new String(chars) + fid_str;
        } else {
            str = fid_str + new String(chars);
        }
        return str;
    }

    public static List<Layer> getAllLayers(DataSource ds) {
        List<Layer> layerList = new ArrayList<Layer>();
        int layerCount = ds.GetLayerCount();
        for (int i = 0; i < layerCount; i++) {
            Layer layer = ds.GetLayer(i);
            layerList.add(layer);
        }
        return layerList;
    }

    public static String getGeoType(StringBuffer wkt) {
        return wkt.substring(0, wkt.indexOf(" "));
    }

    public static void main(String[] args) throws FileNotFoundException {
        Mdb2wktWithProperty mdb2wktWithProperty = new Mdb2wktWithProperty();
        String mdbPath = "\\\\10.2.35.7\\share\\no point\\UNI.gdb";
        DataOutputStream fs = new DataOutputStream(new FileOutputStream("C:\\Users\\Administrator\\Desktop\\UNI3.txt"));
//        String mdbPath = args[0];
//        String txtPath = args[1];
//        DataOutputStream fs = new DataOutputStream(new FileOutputStream(args[1]));
        mdb2wktWithProperty.mdb2wkt(mdbPath, fs);
    }
}
