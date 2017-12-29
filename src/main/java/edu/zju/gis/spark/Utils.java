package edu.zju.gis.spark;

import com.esri.core.geometry.*;
import com.esri.core.geometry.Geometry;
import edu.zju.gis.gncstatistic.*;
import edu.zju.gis.spark.Common.CommonSettings;
import edu.zju.gis.spark.ResultSaver.CompactAlgorithm;
import edu.zju.gis.spark.ResultSaver.ShapeFileWriteThread;
import edu.zju.gis.spark.ParallelTools.RangeCondition.RangeCondition;
import edu.zju.gis.spark.ResultSaver.IResultSaver;
import edu.zju.gis.spark.ResultSaver.OracleResultSaver;
import edu.zju.gis.spark.SurfaceAreaCalculation.ClipFeature;
import edu.zju.gis.spark.SurfaceAreaCalculation.DEMGrid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.htrace.fasterxml.jackson.core.JsonGenerator;
import org.apache.htrace.fasterxml.jackson.core.JsonProcessingException;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.ogr.*;
import scala.Tuple2;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


import java.io.*;
import java.util.*;

import static edu.zju.gis.spark.SurfaceAreaCalculation.DEMGrid.scanLineByBuf;

/**
 * Created by zhy on 2017/8/4.
 */
public class Utils {

    final public static String targetProj75 = "PROJCS[\"CGCS_2000_75\",GEOGCS[\"GCS_CGCS_2000\",DATUM[\"D_2000\",SPHEROID[\"S_2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",75.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0]]";
    final public static String targetProj81 = "PROJCS[\"CGCS_2000_81\",GEOGCS[\"GCS_CGCS_2000\",DATUM[\"D_2000\",SPHEROID[\"S_2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",81.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0]]";
    final public static String targetProj87 = "PROJCS[\"CGCS_2000_87\",GEOGCS[\"GCS_CGCS_2000\",DATUM[\"D_2000\",SPHEROID[\"S_2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",87.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0]]";
    final public static String targetProj93 = "PROJCS[\"CGCS_2000_93\",GEOGCS[\"GCS_CGCS_2000\",DATUM[\"D_2000\",SPHEROID[\"S_2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",93.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0]]";
    final public static String targetProj99 = "PROJCS[\"CGCS_2000_99\",GEOGCS[\"GCS_CGCS_2000\",DATUM[\"D_2000\",SPHEROID[\"S_2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",99.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0]]";
    final public static String targetProj105 = "PROJCS[\"CGCS_2000_105\",GEOGCS[\"GCS_CGCS_2000\",DATUM[\"D_2000\",SPHEROID[\"S_2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",105.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0]]";
    final public static String targetProj111 = "PROJCS[\"CGCS_2000_111\",GEOGCS[\"GCS_CGCS_2000\",DATUM[\"D_2000\",SPHEROID[\"S_2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",111.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0]]";
    final public static String targetProj117 = "PROJCS[\"CGCS_2000_117\",GEOGCS[\"GCS_CGCS_2000\",DATUM[\"D_2000\",SPHEROID[\"S_2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",117.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0]]";
    final public static String targetProj123 = "PROJCS[\"CGCS_2000_123\",GEOGCS[\"GCS_CGCS_2000\",DATUM[\"D_2000\",SPHEROID[\"S_2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",123.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0]]";
    final public static String targetProj129 = "PROJCS[\"CGCS_2000_129\",GEOGCS[\"GCS_CGCS_2000\",DATUM[\"D_2000\",SPHEROID[\"S_2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",129.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0]]";
    final public static String targetProj135 = "PROJCS[\"CGCS_2000_135\",GEOGCS[\"GCS_CGCS_2000\",DATUM[\"D_2000\",SPHEROID[\"S_2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",135.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0]]";
    public static Map<Integer,String> targetProjMap = new HashMap<Integer,String>();
    public static Map<String,Integer> BJSheetCodeMap = new HashMap<String,Integer>(){{
        put("K50E018006",1);
        put("K50E018007",1);
        put("K50E018008",1);
        put("K50E018009",1);
        put("K50E018010",1);
        put("K50E018011",1);
        put("K50E018012",1);
        put("K50E018013",1);
        put("K50E018014",1);
        put("K50E018015",1);
        put("K50E019006",1);
        put("K50E019007",1);
        put("K50E019008",1);
        put("K50E019009",1);
        put("K50E019010",1);
        put("K50E019011",1);
        put("K50E019012",1);
        put("K50E019013",1);
        put("K50E019014",1);
        put("K50E019015",1);
        put("K50E020006",1);
        put("K50E020007",1);
        put("K50E020008",1);
        put("K50E020009",1);
        put("K50E020010",1);
        put("K50E020011",1);
        put("K50E020012",1);
        put("K50E020013",1);
        put("K50E020014",1);
        put("K50E020015",1);
        put("K50E021006",1);
        put("K50E021007",1);
        put("K50E021008",1);
        put("K50E021009",1);
        put("K50E021010",1);
        put("K50E021011",1);
        put("K50E021012",1);
        put("K50E021013",1);
        put("K50E021014",1);
        put("K50E021015",1);
        put("K50E022006",1);
        put("K50E022007",1);
        put("K50E022008",1);
        put("K50E022009",1);
        put("K50E022010",1);
        put("K50E022011",1);
        put("K50E022012",1);
        put("K50E022013",1);
        put("K50E022014",1);
        put("K50E022015",1);
        put("K50E023006",1);
        put("K50E023007",1);
        put("K50E023008",1);
        put("K50E023009",1);
        put("K50E023010",1);
        put("K50E023011",1);
        put("K50E023012",1);
        put("K50E023013",1);
        put("K50E023014",1);
        put("K50E023015",1);
        put("J50E000006",1);
        put("J50E000007",1);
        put("J50E000008",1);
        put("J50E000009",1);
        put("J50E000010",1);
        put("J50E000011",1);
        put("J50E000012",1);
        put("J50E000013",1);
        put("J50E000014",1);
        put("J50E000015",1);
        put("J50E001006",1);
        put("J50E001007",1);
        put("J50E001008",1);
        put("J50E001009",1);
        put("J50E001010",1);
        put("J50E001011",1);
        put("J50E001012",1);
        put("J50E001013",1);
        put("J50E001014",1);
        put("J50E001015",1);
        put("J50E002006",1);
        put("J50E002007",1);
        put("J50E002008",1);
        put("J50E002009",1);
        put("J50E002010",1);
        put("J50E002011",1);
        put("J50E002012",1);
        put("J50E002013",1);
        put("J50E002014",1);
        put("J50E002015",1);
        put("J50E003006",1);
        put("J50E003007",1);
        put("J50E003008",1);
        put("J50E003009",1);
        put("J50E003010",1);
        put("J50E003011",1);
        put("J50E003012",1);
        put("J50E003013",1);
        put("J50E003014",1);
        put("J50E003015",1);
        put("J50E004006",1);
        put("J50E004007",1);
        put("J50E004008",1);
        put("J50E004009",1);
        put("J50E004010",1);
        put("J50E004011",1);
        put("J50E004012",1);
        put("J50E004013",1);
        put("J50E004014",1);
        put("J50E004015",1);

    }};

    final public static Map<String,Integer> ccCode = new HashMap<String,Integer>(){{
        put("",1);

    }};

    static {
        targetProjMap.put(75,edu.zju.gis.spark.Utils.targetProj75);
        targetProjMap.put(81,edu.zju.gis.spark.Utils.targetProj81);
        targetProjMap.put(87,edu.zju.gis.spark.Utils.targetProj87);
        targetProjMap.put(93,edu.zju.gis.spark.Utils.targetProj93);
        targetProjMap.put(99,edu.zju.gis.spark.Utils.targetProj99);
        targetProjMap.put(105,edu.zju.gis.spark.Utils.targetProj105);
        targetProjMap.put(111,edu.zju.gis.spark.Utils.targetProj111);
        targetProjMap.put(117,edu.zju.gis.spark.Utils.targetProj117);
        targetProjMap.put(123,edu.zju.gis.spark.Utils.targetProj123);
        targetProjMap.put(129,edu.zju.gis.spark.Utils.targetProj129);
        targetProjMap.put(135,edu.zju.gis.spark.Utils.targetProj135);


    }

    public static int[] world2Pixel(double[] geoMatrix, double x, double y) {
        /* geoMatrix[0] left-up-x
		 geoMatrix[3] left-up-y
		 geoMatrix[1] x方向分辨率
		 geoMatrix[5]  y方向分辨率
		 geoMatrix[2]  旋转角度 0 表示图像北方朝上
		 geoMatrix[4]  旋转角度 0 表示图像北方朝上*/
        try {
            int[] result = new int[2];
            int pixel = (int) ((geoMatrix[4] * x - geoMatrix[1] * y - geoMatrix[4] * geoMatrix[0] + geoMatrix[1] * geoMatrix[3]) / (geoMatrix[4] * geoMatrix[2] - geoMatrix[1] * geoMatrix[5]));
            int line = (int) ((geoMatrix[5] * x - geoMatrix[2] * y - geoMatrix[5] * geoMatrix[0] + geoMatrix[2] * geoMatrix[3]) / (geoMatrix[5] * geoMatrix[1] - geoMatrix[2] * geoMatrix[4]));
            result[0] = pixel;
            result[1] = line;
            return result;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static double[] pixel2World(double[] geoMatrix, int pixel, int line) {
        try {
            double[] result = new double[2];
            double x = geoMatrix[0] + pixel * geoMatrix[2] + line * geoMatrix[1];
            double y = geoMatrix[3] + pixel * geoMatrix[5] + line * geoMatrix[4];
            result[0] = x;
            result[1] = y;
            return result;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static double[] pixel2World(double[] geoMatrix, double pixel, double line) {
        try {
            double[] result = new double[2];
            double x = geoMatrix[0] + pixel * geoMatrix[2] + line * geoMatrix[1];
            double y = geoMatrix[3] + pixel * geoMatrix[5] + line * geoMatrix[4];
            result[0] = x;
            result[1] = y;
            return result;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static void scanLine(double[] geoTrans, ClipFeature clipFeature, int colNum, int rowNum, Band band) {
        double m_extends[] = new double[4];
        m_extends[0] = clipFeature.getEnv_xmin();
        m_extends[1] = clipFeature.getEnv_xmax();
        m_extends[2] = clipFeature.getEnv_ymin();
        m_extends[3] = clipFeature.getEnv_ymax();
        //将左上角和右下角的角点转换成像素坐标
        int[] ulx_uly = world2Pixel(geoTrans, m_extends[0], m_extends[3]);   //左上角
        int[] lrx_lry = world2Pixel(geoTrans, m_extends[1], m_extends[2]);   //右下角
        int rxsize = lrx_lry[0] - ulx_uly[0]+1;         //这个MBR所占的行数
        int rysize = lrx_lry[1] - ulx_uly[1]+1;         //这个MBR所占的列数
        //TODO 如果这个矢量文件很小 需要判断矢量文件是否超出了栅格范围
        int[][] demrasters = new int[rxsize][rysize];
//        int[][] demrasters = new int[rowNum][colNum];
        Geometry lcraGeo = clipFeature.getGeo();
        //start scanning
        for (int x = ulx_uly[0] + 1; x < lrx_lry[0] + 1; x++) {
            double[] leftPoint = pixel2World(geoTrans, x, ulx_uly[1]);
            double[] rightPoint = pixel2World(geoTrans, x, lrx_lry[1] + 1);
            String wktLine = "LINESTRING(" + leftPoint[0] + " " + leftPoint[1] + "," + rightPoint[0] + " " + rightPoint[1] + ")";
            Geometry lineGeo = GeometryEngine.geometryFromWkt(wktLine, WktImportFlags.wktImportDefaults, Geometry.Type.Unknown);
            Geometry intersect = GeometryEngine.intersect(lineGeo, lcraGeo, null);
            if (intersect != null) {
                Polyline multiline = (Polyline) intersect;
                Point2D[] point2Ds = multiline.getCoordinates2D();
                if (point2Ds.length == 0)
                    continue;
                //TODO 如果nodeCount是奇数
                if (point2Ds.length % 2 == 0) {
                    int tupleNum = point2Ds.length / 2;
                    for (int j = 0; j < tupleNum; j++) {
                        int[] pl1 = Utils.world2Pixel(geoTrans, point2Ds[j * 2].x, point2Ds[j * 2].y);
                        int[] pl2 = Utils.world2Pixel(geoTrans, point2Ds[j * 2 + 1].x, point2Ds[j * 2 + 1].y);
                        if (pl1[0] <= pl2[0]) {
                            for (int k = pl1[1]; k <= pl2[1]; k++) {
                                demrasters[x-1-ulx_uly[0]][k-ulx_uly[1]]=1;
                                demrasters[x-ulx_uly[0]][k-ulx_uly[1]]=1;
                            }
                        } else {
                            for (int k = pl2[1]; k <= pl1[1]; k++) {
                                demrasters[x-1-ulx_uly[0]][k-ulx_uly[1]]=1;
                                demrasters[x-ulx_uly[0]][k-ulx_uly[1]]=1;
                            }
                        }
                    }
                } else {
                    System.out.println("该扫描线与多边形的交点有 " + point2Ds.length + "个");
                }
            }
        }
        //现在已经获得了这个要素的对应的栅格   每次计算都去读这个文件？？还是把这个文件读到内存里来？
        //计算地表面积
        double featureArea = 0;
        featureArea = featureAreaCalculation(demrasters, clipFeature, lcraGeo, geoTrans, band,ulx_uly[0],ulx_uly[1]);
        System.out.println("该要素总面积为："+featureArea);
    }
    //计算地表面积    xmin ymin是这个demrasters的起始行号和列号
    public static double featureAreaCalculation(int[][] demrasters, ClipFeature clipfeat, Geometry geo, double[] geoTrans, Band band,int xmin,int ymin) {
        String tfCode = clipfeat.getTFcode();
        String demFilePath = getDEMFilePathByTF(10, tfCode);
        double area = 0;
        for(int i=0;i<demrasters.length;i++){
            for(int j=0;j<demrasters[0].length;j++){
                if(demrasters[i][j]==1){
                    //进一步判断这个格网是否是全包含关系
                    DEMGrid demGrid = new DEMGrid(i+xmin,j+ymin,geoTrans,band);
                    if(demGrid.IsFullContain(demrasters,i,j)){
                        double subArea = demGrid.calAreaInGrid(geo,true);

//                        System.out.println(subArea);
                        area+=subArea;
                    }else{
                        double subArea = demGrid.calAreaInGrid(geo,false);
//                        System.out.println(subArea);
                        area+=subArea;
                    }
                }
            }
        }
        return area;
    }

    public static String getDEMFilePathByTF(int gridsize, String TFcode) {
        String demFilePath = null;
        if (gridsize == 2) {
            demFilePath = "";
            return demFilePath;
        }
        if (gridsize == 10) {
//            demFilePath = "E:\\zhy\\dem\\dem.tif";
//            demFilePath = "E:\\zhy\\raster\\raster1.tif";
            demFilePath ="/zhy/testdata/demtest.tif";
            return demFilePath;
        }
        return "error";
    }

    //filePath = hdfs://192.168.1.105:9000/zhy/lcra_sheet_cut2/H51
    public static List<String> GetAllSheetCode(String filePath) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] statuses = fs.listStatus(new Path(filePath));
        List<String> sheetCodeList =new ArrayList<String>();
        //第一层是文件夹
        for(int i=0;i<statuses.length;i++){
            FileStatus[] fss = fs.listStatus(new Path(statuses[i].getPath().toString()));
            for(FileStatus f:fss){
                String name = f.getPath().getName();
//                if(name.length()!=10||!BJSheetCodeMap.containsKey(name))
                if(name.length()!=10)
                    continue;
                sheetCodeList.add(f.getPath().getName());
            }
        }
        return sheetCodeList;
    }

    public static String GetDemPathBySheetCode(String sheetCode,String demDir){
//        return demDir+"/"+sheetCode.substring(0,3)+"/"+sheetCode+"/"+sheetCode.toLowerCase()+"dem";
        return demDir+"/"+sheetCode.substring(0,3)+"/"+sheetCode+".tif";
    }

    public static List<Geometry> cutPolygon(Geometry geometry,double cutSize){
        Envelope2D boundary = new Envelope2D();
        geometry.queryEnvelope2D(boundary);
        Point p1 = boundary.getLowerLeft();
        Point p2 = boundary.getUpperRight();
        double xmin = p1.getX();
        double ymin = p1.getY();
        double xmax = p2.getX();
        double ymax = p2.getY();
        double xrange = xmax-xmin;
        double yrange = ymax-ymin;
        int rowNum = (int)(yrange/cutSize)+1;
        int colNum = (int)(xrange/cutSize)+1;
        Geometry cGeo = null;
        List<Geometry> geoList = new ArrayList<Geometry>();
        for(int i=0;i<rowNum;i++){
            for(int j=0;j<colNum;j++){
                double _xmin = xmin+j*cutSize;
                double _xmax = _xmin+cutSize;
                double _ymin = ymin+i*cutSize;
                double _ymax = _ymin+cutSize;
                Envelope env = new Envelope(_xmin,_ymin,_xmax,_ymax);
                cGeo = GeometryEngine.intersect(geometry,env,null);
                if(!cGeo.isEmpty())
                    geoList.add(cGeo);
            }
        }
        return geoList;
    }

    public static Map<String, String> ParseParameters(String parameterStr)
    {
        String[] parameterTuple = parameterStr.split("&");
        Map keyValues = new HashMap();
        for (int i = 0; i < parameterTuple.length; i++) {
            String[] tuple = parameterTuple[i].split("=", 2);
            keyValues.put(tuple[0].toLowerCase(), tuple[1]);
        }
        return keyValues;
    }

    public static Map<String, String> ParseParameters(String[] parameterStrArr)
    {
        Map keyValues = new HashMap();
        if (parameterStrArr.length < 1) {
            System.out.println("参数必须大于等于1个");
            try {
                throw new Exception("输入参数不合法！");
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            keyValues.put("applicationid", parameterStrArr[0]);
        }

        for (int i = 1; i < parameterStrArr.length; i++) {
            System.out.println(parameterStrArr[i]);
            keyValues.putAll(ParseParameters(parameterStrArr[i]));
        }
        return keyValues;
    }

    public static void SaveResultInOracle(String applicationId, Object result)
            throws JsonProcessingException, IOException
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonGenerator jsonGenerator = objectMapper.getJsonFactory().createJsonGenerator(output);
        jsonGenerator.writeObject(result);
        String resultStr = output.toString();
        System.out.println(resultStr);
        output.close();
        IResultSaver saver = new OracleResultSaver();
        try {
            if (saver.SaveResult(applicationId, resultStr))
                System.out.println("结果保存成功");
            else
                System.out.println("结果保存失败");
        } catch (Exception ex) {
            System.out.println("结果保存失败：" + ex.toString());
        }
    }

    public static void SaveResultInOracle(String applicationId, Object result,boolean isPath)
            throws JsonProcessingException, IOException
    {
        String resultStr;
        if(!isPath){
            //如果不是路径结果 要转成json存到oracle
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            ObjectMapper objectMapper = new ObjectMapper();
            JsonGenerator jsonGenerator = objectMapper.getJsonFactory().createJsonGenerator(output);
            jsonGenerator.writeObject(result);
            resultStr = output.toString();
            output.close();
        }else{
            //如果是路径结果  直接存进oracle
            String[] values = ((String)result).split("#");
            resultStr = "{\""+values[0]+"\":\""+values[1]+"\"}";
        }
        System.out.println(resultStr);
        IResultSaver saver = new OracleResultSaver();
        try {
            if (saver.SaveResult(applicationId, resultStr))
                System.out.println("结果保存成功");
            else
                System.out.println("结果保存失败");
        } catch (Exception ex) {
            System.out.println("结果保存失败：" + ex.toString());
        }
    }

    /**
     * 将传入的CC码过滤器转换成map
     * @param ccFilterStr 以逗号分隔的cc码（四位）
     * @return
     */
    public static Map<String,Integer> ccFliterFormat(String ccFilterStr){
        String[] filterValues = ccFilterStr.split(",");
        Map<String,Integer> ccFilterMap = new HashMap<String,Integer>();
        for(String cc:filterValues){
            ccFilterMap.put(cc,1);
        }
        return ccFilterMap;
    }

    public static void regionCodeFilterFormat(String[] rowkeyArr,Map<String,Integer> provinceFilter,Map<String,Integer> cityFilter,Map<String,Integer> countyFilter){
        for(String rowkey:rowkeyArr){
            String regionCC = rowkey.substring(12,16);
            String regionCode = rowkey.substring(0,6);
            if(regionCC.equals("1112")){
                provinceFilter.put(regionCode,0);
            }
            else if(regionCC.equals("1114")){
                cityFilter.put(regionCode,0);
            }
            else if(regionCC.equals("1115")){
                countyFilter.put(regionCode,0);
            }
        }
    }

    public static void parallelResultOutput(JavaRDD<String> bufferResult, String outputFormat,String modelName, String applicationID) throws IOException {
        String outputFileName =modelName.toUpperCase()+"_"+applicationID;  //输出的文件名（注意不是文件夹名）
        String outputPath;
        String outputResult=null;
        File file = new File(CommonSettings.DEFAULT_LOCAL_OUTPUT_PATH+applicationID);
        if(file.exists()){
            //不能直接地file.delete  要递归的删除下面的所有文件
            boolean flag = deleteDir(file);
            if(flag){
                System.out.println("删除文件夹"+file.getPath());
            }
        }
        if(!file.exists()&&!file.isDirectory()){
            file.mkdir();
            System.out.println("新建文件夹"+file.getPath());
        }

        if(outputFormat.equals("hdfswkt")){
//            bufferResult.coalesce(1,true).saveAsTextFile(outputPath);
            outputPath = CommonSettings.DEFAULT_HDFS_OUTPUT_PATH+outputFileName+".txt";
            bufferResult.repartition(1).saveAsTextFile(outputPath);
            outputResult = "hdfspath#"+outputPath;
        }
        else if(outputFormat.equals("LOCAL:WKT")){

        }
        else if(outputFormat.equals("localshp")){
            List<String> geo_buffer=bufferResult.collect();
            if(geo_buffer.size()==0) return;
//            //mkdir outpath
//            File file =new File(CommonSettings.DEFAULT_LOCAL_OUTPUT_PATH+applicationID);
            outputPath = file.getPath();
            if(outputFormat.equals("shp")){
//                //write to shapefile
                ShapeFileWriteThread writeThread=new ShapeFileWriteThread();
                if(!outputFileName.endsWith(".shp")){
                    outputFileName+=".shp";
                }
//                String spatialRef = targetProj117;
//                org.gdal.osr.SpatialReference srs = getSRS("GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433],AUTHORITY[\"EPSG\",4326]]");
               org.gdal.osr.SpatialReference srs = getSRS("GEOGCS[\"GCS_China_Geodetic_Coordinate_System_2000\",DATUM[\"D_China_2000\",SPHEROID[\"CGCS2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433],METADATA[\"China\",73.62,16.7,134.77,53.55,0.0,0.0174532925199433,0.0,1067],AUTHORITY[\"EPSG\",4490]]");
//              org.gdal.osr.SpatialReference srs = getSRS(targetProj117);
                writeThread.driver= CommonSettings.ogrSetup();
                writeThread.jobName=applicationID;
                writeThread.srs=srs;
                writeThread.ds_name=outputFileName;
                writeThread.results=geo_buffer;

                new Thread(writeThread).start();
            }else{
                FileWriter fw = new FileWriter(new File(file.getPath()+"/"+outputFileName));
                for(String s : geo_buffer){
                    fw.write(s+"\n");
                }
                fw.close();
            }
            //shp文件夹打包
            String outputZip = CommonSettings.DEFAULT_LOCAL_OUTPUT_PATH+applicationID+".zip";
//            new CompactAlgorithm(new File(outputZip)).zipFiles(file);
            File[] files = file.listFiles();
            File zipFile = new File(outputZip);
            ZipFiles(zipFile,CommonSettings.DEFAULT_LOCAL_OUTPUT_PATH,files);
            outputResult = "localpath#"+outputZip;
        }
        edu.zju.gis.spark.Utils.SaveResultInOracle(applicationID,outputResult,true);
    }

    public static Map<String, Integer> splitTask(int perTaskNum,List<String> gridKeys){
        int jobNum = gridKeys.size()/(perTaskNum-1);
        int num = 0;
        int circle = 1;
        Map<String, Integer> taskSchema = new HashMap<String, Integer>();
        for (int i = 0; i < gridKeys.size(); i++) {
            num++;
            if (num <= perTaskNum) {
                taskSchema.put(gridKeys.get(i), circle);
            } else if (circle < jobNum) {
                circle++;
                taskSchema.put(gridKeys.get(i), circle);
                num = 1;
            } else {
                // 处理最后一个分片可能会多出来的个数
                taskSchema.put(gridKeys.get(i), circle);
            }
        }
        return taskSchema;
    }



    public static JavaPairRDD<String, Iterable<String>> getSpatialLcra(List<Grid> grids, Map<String,Integer> ccFliterMap, JavaSparkContext sc, String lcraGridPath){
        final Broadcast<Map<String,Integer>> ccFilterBC = sc.broadcast(ccFliterMap);
        JavaPairRDD<String, Iterable<String>> lcraRDD = Utils_zhy.GetSpatialRDD_ccfilterd(sc, grids,ccFilterBC.getValue(),lcraGridPath); // 获取含CC码的Key的地类图斑格网内数据
        return lcraRDD;
    }

    public static JavaPairRDD<String, Iterable<String>> getSpatialLcra(List<Sheet> sheets, Map<String,Integer> ccFliterMap, JavaSparkContext sc,String lcraGridPath,int a){
        System.out.println("sheetList size = "+sheets.size());
        final Broadcast<Map<String,Integer>> ccFilterBC = sc.broadcast(ccFliterMap);
        JavaPairRDD<String, Iterable<String>> lcraRDD = Utils_zhy.GetSpatialRDD_ccfilterd(sc, sheets,ccFilterBC.getValue(),lcraGridPath,1); // 获取含CC码的Key的地类图斑格网内数据
        return lcraRDD;
    }


    /**
     * 获得计算单元的范围
     */
    public static JavaPairRDD<String, Iterable<Tuple2<String, Geometry>>> getExtention(RangeCondition rangeCondition, JavaSparkContext sc){
        JavaPairRDD<String, Iterable<Tuple2<String, Geometry>>> gridRDD = rangeCondition.convertRange2Grid(sc);
        return gridRDD;
    }

    public static JavaPairRDD<String,Iterable<Tuple2<String, Geometry>>> getSheetExtention(RangeCondition rangeCondition,JavaSparkContext sc){
        JavaPairRDD<String,Iterable<Tuple2<String, Geometry>>> sheetRDD =rangeCondition.convertRange2Sheet(sc);
        return sheetRDD;
    }

    public static org.gdal.osr.SpatialReference getSRS(String wkt){
        org.gdal.osr.SpatialReference srs = new org.gdal.osr.SpatialReference();
        srs.ImportFromWkt(wkt);
        return srs;
    }

    public static String rightPad(String str,int size){
        for(int i=0;i<size;i++){
            str+="0";
        }
        return str;
    }

    public static List<String> readShp2WktList(String shapepath){
        ogr.RegisterAll();
        DataSource ds = ogr.Open(shapepath);
        System.out.println("shapepath="+shapepath);
        Layer layer = ds.GetLayer(0);
        List<String> wktList = new ArrayList<String>();
        Feature feature=null;
        while ((feature = layer.GetNextFeature()) != null){
            org.gdal.ogr.Geometry geometry = feature.GetGeometryRef();
            String wkt = geometry.ExportToWkt();
            wktList.add(wkt);
        }
        return wktList;
    }

    public static List<String> readTxt2WktList() throws IOException {
        List<String> wktList = new ArrayList<String>();
        File file  = new File(CommonSettings.DEFAULT_LOCAL_INPUT_PATH+"wkt.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        String str = null;
        while ((str=br.readLine())!=null){
            wktList.add(str);
        }
        return wktList;
    }

    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }

    public static boolean fileToZip(String sourceFilePath,String zipFilePath,String fileName){
        boolean flag = false;
        File sourceFile = new File(sourceFilePath);
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        FileOutputStream fos = null;
        ZipOutputStream zos = null;

        if(sourceFile.exists() == false){
            System.out.println("待压缩的文件目录："+sourceFilePath+"不存在.");
        }else{
            try {
                File zipFile = new File(zipFilePath + "/" + fileName +".zip");
                if(zipFile.exists()){
                    System.out.println(zipFilePath + "目录下存在名字为:" + fileName +".zip" +"打包文件.");
                }else{
                    File[] sourceFiles = sourceFile.listFiles();
                    if(null == sourceFiles || sourceFiles.length<1){
                        System.out.println("待压缩的文件目录：" + sourceFilePath + "里面不存在文件，无需压缩.");
                    }else{
                        fos = new FileOutputStream(zipFile);
                        zos = new ZipOutputStream(new BufferedOutputStream(fos));
                        byte[] bufs = new byte[1024*10];
                        for(int i=0;i<sourceFiles.length;i++){
                            //创建ZIP实体，并添加进压缩包
                            ZipEntry zipEntry = new ZipEntry(sourceFiles[i].getName());
                            zos.putNextEntry(zipEntry);
                            //读取待压缩的文件并写进压缩包里
                            fis = new FileInputStream(sourceFiles[i]);
                            bis = new BufferedInputStream(fis, 1024*10);
                            int read = 0;
                            while((read=bis.read(bufs, 0, 1024*10)) != -1){
                                zos.write(bufs,0,read);
                            }
                        }
                        flag = true;
                    }
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally{
                //关闭流
                try {
                    if(null != bis) bis.close();
                    if(null != zos) zos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }
        return flag;
    }

    public static void ZipFiles(File zip,String path,File... srcFiles) throws IOException{
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zip));
        ZipFiles(out,path,srcFiles);
        out.close();
        System.out.println("*****************压缩完毕*******************");
    }


    /**
     * 压缩文件-File
     * @param srcFiles 被压缩源文件
     * @author isea533
     */
    public static void ZipFiles(ZipOutputStream out,String path,File... srcFiles){
        path = path.replaceAll("\\*", "/");
        if(!path.endsWith("/")){
            path+="/";
        }
        byte[] buf = new byte[1024];
        try {
            for(int i=0;i<srcFiles.length;i++){
                if(srcFiles[i].isDirectory()){
                    File[] files = srcFiles[i].listFiles();
                    String srcPath = srcFiles[i].getName();
                    srcPath = srcPath.replaceAll("\\*", "/");
                    if(!srcPath.endsWith("/")){
                        srcPath+="/";
                    }
                    out.putNextEntry(new ZipEntry(path+srcPath));
                    ZipFiles(out,path+srcPath,files);
                }
                else{
                    FileInputStream in = new FileInputStream(srcFiles[i]);
                    System.out.println(path + srcFiles[i].getName());
                    out.putNextEntry(new ZipEntry(path + srcFiles[i].getName()));
                    int len;
                    while((len=in.read(buf))>0){
                        out.write(buf,0,len);
                    }
                    out.closeEntry();
                    in.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public static void main(String[] args) {
        ogr.RegisterAll();
        gdal.AllRegister();
        //==========读取tif文件===========
        Dataset hds = gdal.Open("E:\\zhy\\raster\\raster1.tif");
//        Dataset hds = gdal.Open("E:\\zhy\\dem\\dem.tif");
        double[] geoTrans = hds.GetGeoTransform();
        Band band = hds.GetRasterBand(1);
        int gridXCount = hds.getRasterXSize();    //列数
        int gridYCount = hds.getRasterYSize();    //行数
        float[] buf = new float[gridXCount*gridYCount];
        band.ReadRaster(0,0,gridXCount,gridYCount,buf);

        //==============读取shp文件===============
        DataSource ds = ogr.Open("E:\\zhy\\raster\\lcra.shp", false);
//        DataSource ds  = ogr.Open("E:\\zhy\\shp\\partLCRA.shp",false);
        Layer layer = ds.GetLayer(0);
        Feature feature = null;
        int featCount = 0;
        Date start = new Date();
        while ((feature = layer.GetNextFeature()) != null) {
            System.out.println("============正在扫描第 " + featCount + " 个要素=================");
            org.gdal.ogr.Geometry geometry = feature.GetGeometryRef();
            Geometry geo= GeometryEngine.geometryFromWkt(geometry.ExportToWkt(),0,Geometry.Type.Unknown);
            cutPolygon(geo,10);
            ClipFeature clipFeature = new ClipFeature(geometry,"ID");
            scanLineByBuf(geoTrans, clipFeature, gridXCount, gridYCount, buf);
            featCount++;
        }
        Date end = new Date();
        System.out.println("所需时间为： " + (end.getTime() - start.getTime()) / 1000.00 + " 秒");
    }
}
