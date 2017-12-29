package edu.zju.gis.spark.TransformationTool;

import org.gdal.gdal.gdal;
import org.gdal.ogr.DataSource;
import org.gdal.ogr.Feature;
import org.gdal.ogr.Layer;
import org.gdal.ogr.ogr;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhy on 2017/6/24.
 */
public class MdbFeatureCount {

    public MdbFeatureCount() {
        ogr.RegisterAll();
        gdal.AllRegister();
        gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "NO");
        gdal.SetConfigOption("SHAPE_ENCODING", "GB2312");
    }
//    public void setup(){
//        ogr.RegisterAll();
//        gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "NO");
//        gdal.SetConfigOption("SHAPE_ENCODING", "GB2312");
//    }

    /**
     * mdb转wkt
     *
     * @param mdbFilePath
     * @param layerName
     *
     */
    public long mdbFeatureCount(String mdbFilePath, String layerName) {

        long featCount = -1;
        try {
            DataSource ds = ogr.Open(mdbFilePath, false);
            if (ds == null) {
                return 0;
            }
            Layer player = ds.GetLayerByName(layerName);
            if (player != null) {
                featCount =0;
                Feature feature;
                while ((feature = player.GetNextFeature()) != null) {
                    featCount++;
                }
            }

        } catch (Exception e) {
            String errorMessage = "ERROR:mdbPath=" + mdbFilePath + ",message=" + e.getMessage();
            return 0;
        }
        return featCount;
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
