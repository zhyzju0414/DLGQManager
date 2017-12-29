package edu.zju.gis.spark.Common;

import org.gdal.gdal.gdal;
import org.gdal.ogr.Driver;
import org.gdal.ogr.ogr;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Zhy on 2017/11/5.
 */
public class CommonSettings {
    public static String DEFAULT_HDFS_OUTPUT_PATH;
    public static String DEFAULT_LOCAL_OUTPUT_PATH;
    public static String DEFAULT_INPUT_DELIMITER;
    public static String DEFAULT_OUTPUT_DELIMITER;
    public static int DEFAULT_WKID;
    public static String DEFAULT_HDFS_PATH;
    public static String DEFAULT_SHARE_OUTPUT_PATH;
    public static String DEFAULT_CONFIG_PATH;
    public static String DEFAULT_GRID_DIR;
    public static String DEFAULT_LOCAL_INPUT_PATH;
    public static String GEOFILE_NAME;
    public static String PROPERTY_FILENAME;
    public static String SPATIAL_INDEX_FILENAME;


    static{
        Properties prop = new Properties();
        InputStream in = CommonSettings.class.getResourceAsStream("resources.properties");
        try{
            prop.load(in);
            DEFAULT_INPUT_DELIMITER = prop.getProperty("DEFAULT_INPUT_DELIMITER");
            DEFAULT_OUTPUT_DELIMITER = prop.getProperty("DEFAULT_OUTPUT_DELIMITER");
            DEFAULT_HDFS_OUTPUT_PATH = prop.getProperty("DEFAULT_HDFS_OUTPUT_PATH").trim();
            DEFAULT_HDFS_PATH=prop.getProperty("DEFAULT_HDFS_PATH").trim();
            DEFAULT_LOCAL_OUTPUT_PATH = prop.getProperty("DEFAULT_LOCAL_OUTPUT_PATH").trim();
            DEFAULT_SHARE_OUTPUT_PATH = prop.getProperty("DEFAULT_SHARE_OUTPUT_PATH").trim();
            DEFAULT_CONFIG_PATH = prop.getProperty("DEFAULT_CONFIG_PATH").trim();
            DEFAULT_WKID = Integer.parseInt(prop.getProperty("DEFAULT_WKID").trim());
            DEFAULT_GRID_DIR = prop.getProperty("DEFAULT_GRID_DIR").trim();
            DEFAULT_LOCAL_INPUT_PATH = prop.getProperty("DEFAULT_LOCAL_INPUT_PATH").trim();
            GEOFILE_NAME = prop.getProperty("GEOFILE_NAME").trim();
            PROPERTY_FILENAME = prop.getProperty("PROPERTY_FILENAME").trim();
            SPATIAL_INDEX_FILENAME = prop.getProperty("SPATIAL_INDEX_FILENAME").trim();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Driver ogrSetup(){
        // 注册所有的驱动
        ogr.RegisterAll();
        // 为了支持中文路径，请添加下面这句代码
        gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8","YES");
        // 为了使属性表字段支持中文，请添加下面这句
        gdal.SetConfigOption("SHAPE_ENCODING","GB2312");
        //"ESRI Shapefile"
        Driver driver = ogr.GetDriverByName("ESRI Shapefile");
        if(driver==null)
        {
            System.out.println("can not init driver!");
        }
        return driver;
    }
}
