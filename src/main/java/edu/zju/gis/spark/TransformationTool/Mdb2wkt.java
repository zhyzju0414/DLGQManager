package edu.zju.gis.spark.TransformationTool;

import org.apache.commons.lang.StringUtils;
import org.gdal.gdal.gdal;
import org.gdal.ogr.*;
import org.jboss.netty.util.internal.StringUtil;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhy on 2017/6/24.
 */
public class Mdb2wkt {
    //国家局的投影
    public final static String WGS84_proj = "PROJCS[\"WGS_1984_Web_Mercator\",GEOGCS[\"GCS_WGS_1984_Major_Auxiliary_Sphere\",DATUM[\"D_WGS_1984_Major_Auxiliary_Sphere\",SPHEROID[\"WGS_1984_Major_Auxiliary_Sphere\",6378137.0,0.0]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Mercator\"],PARAMETER[\"false_easting\",0.0],PARAMETER[\"false_northing\",0.0],PARAMETER[\"central_meridian\",0.0],PARAMETER[\"standard_parallel_1\",0.0],UNIT[\"Meter\",1.0],AUTHORITY[\"EPSG\",3785]]";


    public Mdb2wkt() {
        ogr.RegisterAll();
        gdal.AllRegister();
        gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "NO");
        gdal.SetConfigOption("SHAPE_ENCODING", "GB2312");
    }

    /**
     * mdb转wkt
     *
     * @param mdbFilePath
     * @param layerName
     * @param trgPrjDef
     * @param fileOutputStream
     */
    public String mdb2wkt(String mdbFilePath, String layerName, String trgPrjDef, DataOutputStream fileOutputStream) {

        BufferedOutputStream bufferedInputStream = new BufferedOutputStream(fileOutputStream);
        org.gdal.osr.SpatialReference sr2 = new org.gdal.osr.SpatialReference();
        boolean geo = false;
        if (trgPrjDef != null) {
            geo = true;
            java.util.Vector vecotr = new java.util.Vector();
            vecotr.add(trgPrjDef);
            sr2.ImportFromESRI(vecotr);
//            sr2.ImportFromEPSG(Integer.parseInt(trgPrjDef));
        }

        StringBuffer stringBuffer = new StringBuffer();
        int flushCount = 0;
        int flushToralence = 50;
        int featCount = 0;

        try {
            DataSource ds = ogr.Open(mdbFilePath, false);
//            String mdbName_rc = mdbFilePath.substring(mdbFilePath.lastIndexOf("_"),mdbFilePath.lastIndexOf("."));
            if (ds == null) {
                return "ERROR" + "open [" + mdbFilePath + "] failed";
            }

            Layer player = ds.GetLayerByName(layerName);

            if (player != null) {
                FeatureDefn ftd = player.GetLayerDefn();
                String[] fieldNames = new String[ftd.GetFieldCount()];
                for (int fieldIndex = 0; fieldIndex < fieldNames.length; fieldIndex++) {
                    fieldNames[fieldIndex] = ftd.GetFieldDefn(fieldIndex).GetName();
                }

               // int featCount = 0;
                Feature feature = null;
                String regionCode;
                String DLBM;
                double area;

                while ((feature = player.GetNextFeature()) != null) {
//                    System.out.println("featcount= "+featCount);
                    featCount++;
                    flushCount++;


                    //过滤小图斑
//                    area = feature.GetFieldAsDouble("SHAPE_Area");
//                    if(area<1500000)
//                        continue;
                    //ID 行政编码（12）+cc_code(4)+FID(8)
                    regionCode = feature.GetFieldAsString("AREACODE");   //导出lcra文件用到的是areacode
//                    regionCode = feature.GetFieldAsString("PAC");   //12位长度   导出乡镇文件 以及高程文件用到的是PAC
//                    regionCode = feature.GetFieldAsString("NTH");   //10位长度  导出格网数据
                    DLBM = feature.GetFieldAsString("CC");    //导出lcra文件和乡镇文件用到的是cc码
//                    DLBM = feature.GetFieldAsString("gridcode");   //导出高程文件用到的是高程值gridcode

//                    if (regionCode.substring(0, 2).equals("51")| regionCode.substring(0, 2).equals("65")) {     //导出乡镇区划时
                    //地类图斑数据暂时没有行政区代码
                    if (regionCode == null)
                        regionCode = "999999999";
                    else
                        regionCode = fillStirng(regionCode, 12, false);
                    stringBuffer.append(regionCode);
                    if (DLBM == null)
                        DLBM = "9999";
                    if(DLBM.length()>4)
                        continue;
                    stringBuffer.append(fillStirng(DLBM, 4, true));
//                    stringBuffer.append(DLBM);
                    stringBuffer.append(fillStirng(String.valueOf(featCount), 8, true)).append("\t");

                    //属性值
                    for (int fieldindex = 0; fieldindex < fieldNames.length; fieldindex++) {
                        String replacedStr = replaceBlank(feature.GetFieldAsString(fieldindex));
                        stringBuffer.append(replacedStr).append("\t");
                    }

                    //几何值

                    if (feature.GetGeomFieldCount() > 0 && feature.GetFieldCount() > 0) {
                        StringBuffer geometryWkt = new StringBuffer();
                        for (int geoIndex = 0; geoIndex < feature.GetGeomFieldCount(); geoIndex++) {

                            Geometry pGeo = feature.GetGeomFieldRef(geoIndex);
//
//                            if (geo) {
//                                pGeo.TransformTo(sr2);
//                            }
                            geometryWkt.append(pGeo.ExportToWkt());
                        }
                        stringBuffer.append(geometryWkt).append("\t");
                    }
                    stringBuffer.deleteCharAt(stringBuffer.length() - 1);

                    if (flushCount == flushToralence) {
                        //输出
                        System.out.println("flush on "+featCount);
                        bufferedInputStream.write(String.valueOf(stringBuffer).getBytes());
                        bufferedInputStream.flush();
                        flushCount = 0;
                        stringBuffer = new StringBuffer();
                    }
                    stringBuffer.append("\r\n");
//                    }
                }

                //剩余输出
                bufferedInputStream.write(String.valueOf(stringBuffer).getBytes());
                bufferedInputStream.flush();
                bufferedInputStream.close();
            }else {
                System.out.println("layer为空"+mdbFilePath);
            }
        } catch (Exception e) {
            String errorMessage = "ERROR:mdbPath=" + mdbFilePath + ",message=" + e.getMessage()+"  featurecount="+featCount+"  errror:"+stackTraceToString(e);
            return errorMessage;
        }
        return "SUCCESS";
    }

    public  static  String stackTraceToString(Exception e){
        StringBuffer sb= new StringBuffer();
        StackTraceElement[] stackTraceElements = e.getStackTrace();
        for (int i = 0; i <stackTraceElements.length ; i++) {
            sb.append(stackTraceElements[i].toString()+"\n");
        }
        return  sb.toString();
    }

    public static String replaceBlank(String str){
        String dest ="";
        if(str!=null){
            Pattern p = Pattern.compile("(\r\n|\r|\n|\n\r)");
            Matcher m = p.matcher(str);
            dest = m.replaceAll("");
        }
        return dest;
    }

    public static String fillStirng(String fid_str, int totalLength, boolean isFrontOrNot){
//        String fid_str = String.valueOf(FID);
        int str_len = fid_str.length();
        char[] chars = new char[totalLength-str_len];
        for(int i = 0;i<chars.length;i++){
            chars[i] = '0';
        }
        String str;
        if(isFrontOrNot){
            str = new String(chars)+fid_str;
        }else{
            str = fid_str+new String(chars);
        }
        return str;
    }


    public static void main(String[] args) {

    }
}
