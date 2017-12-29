package edu.zju.gis.hbase.tool;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.internal.Trees.This;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.Geometry.Type;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;



/**
 * 该类为工具类，提供其他类依赖的基本方法，属性
 * @author xiaobao
 *
 */
public class Utils {
    @Deprecated
    public static final int DELIMITER_NUM = 7;//县级行政区划路径深度
    public static String DEFAULT_INPUT_DELIMITER = "\t";//输入文件的默认分隔符
    public static String DEFAULT_OUTPUT_DELIMITER = "\t";//输出文件的默认分隔符

    public static String SPATIALINDEX_TABLE = "SPATIALINDEX_TAB"; //空间索引表

    public static byte [] COLUMNFAMILY_FID = Bytes.toBytes("CFFID");  //空间索引表的索引列簇

    public static byte [] COLUMNFAMILY_PROPERTY = Bytes.toBytes("CFProperty");  //属性列簇
    public static byte []  CAPTION_QUALIFIER= Bytes.toBytes("Caption");  //标题qualifier
    public static byte []  PROPERTY_QUALIFIER= Bytes.toBytes("Property");  //qualifier
    public static byte []  POSITION_QUALIFIER= Bytes.toBytes("Position");  //qualifier
    public static byte []  FEATURETYPE_QUALIFIER= Bytes.toBytes("FeatureType");  //qualifier
    public static byte []  MBR_QUALIFIER= Bytes.toBytes("MBR");  //qualifier

    public static byte []  COLUMNFAMILY_GEOMETRY = Bytes.toBytes("CFGeometry");  //几何字段列簇
    public static byte []  GEOMETRY_QUALIFIER= Bytes.toBytes("Geometry");  //qualifier

    public static byte []  COLUMNFAMILY_STATINDEX = Bytes.toBytes("CFStatIndex");  //统计字段列簇
    public static Map<Integer,Integer> REGIONCOUNT = new HashMap<Integer,Integer>(){{
        put(11,16 );
        put(12,16 );
        put(13,170);
        put(14,117);
        put(15,101);
        put(21,100);
        put(22,60 );
        put(23,115);
        put(31,17 );
        put(32,93 );
        put(33,90 );
        put(34,78 );
        put(35,84 );
        put(36,99 );
        put(37,135);
        put(41,159);
        put(42,103);
        put(43,118);
        put(44,121);
        put(45,95 );
        put(46,19 );
        put(50,38 );
        put(51,183);
        put(52,88 );
        put(53,129);
        put(54,74 );
        put(61,107);
        put(62,90 );
        put(63,43 );
        put(64,22 );
        put(65,97 );
    }};

    public static Map<Integer,Integer> REGIONNUM = new HashMap<Integer,Integer>(){{
        put(11,1   );
        put(12,17  );
        put(13,33  );
        put(14,203 );
        put(15,320 );
        put(21,421 );
        put(22,521 );
        put(23,581 );
        put(31,696 );
        put(32,713 );
        put(33,806 );
        put(34,896 );
        put(35,974 );
        put(36,1058);
        put(37,1157);
        put(41,1292);
        put(42,1451);
        put(43,1554);
        put(44,1672);
        put(45,1793);
        put(46,1888);
        put(50,1907);
        put(51,1945);
        put(52,2128);
        put(53,2216);
        put(54,2345);
        put(61,2419);
        put(62,2526);
        put(63,2616);
        put(64,2659);
        put(65,2681);
    }};


    //99表示未分类
    public static String[] CC = {"00","01","02","03","04","05","06","07","08","09","10","11","12","20","99"};  //地理国情一级分类
//    public static String[] CC = {"0110","0120","0211","0212","0220","0230","0250","0290","0291","0311","0312","0313","0322","0330","0340","0360","0411","0422","0424","0429","0511","0512","0521","0522","0540","0541","0542","0543","0550","0601","0610","0710","0711","0712","0713","0715","0717","0718","0719","0721","0750","0760","0762","0770","0790","0810","0822","0830","0831","0832","0833","0839","0890","0920","0930","0940","0950","1001","1012"};  //测试数据里面的编码

    public static int DEFAULT_WKID = 4490;//默认空间参考WKID

    //平面投影坐标 CGCS2000椭球，高斯克吕格投影6度带 带号 20
    public static String CGCSPROJ = "PROJCS[\"CGCS2000_GK_Zone_20\",GEOGCS[\"GCS_China_Geodetic_Coordinate_System_2000\",DATUM[\"China_2000\",SPHEROID[\"CGCS2000\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",20500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",117.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0],AUTHORITY[\"EPSG\",\"4498\"]]";

    //经纬度坐标 CGCS2000椭球
    public static String CGCS2000 = "GEOGCS[GCS_China_Geodetic_Coordinate_System_2000,DATUM[D_China_2000,SPHEROID[CGCS2000,6378137.0,298.257222101]],PRIMEM[Greenwich,0.0],UNIT[Degree,0.0174532925199433],METADATA[China,73.62,16.7,134.77,53.55,0.0,0.0174532925199433,0.0,1067],AUTHORITY[EPSG,4490]]";


//    public static Tuple3<Integer,Long,Long> UNIVERSEGRID = new  Tuple3<Integer,Long,Long>(5,13L,10L); //全局搜索范围所处的格网
    public static Tuple3<Integer,Long,Long> UNIVERSEGRID = new  Tuple3<Integer,Long,Long>(1,0L,0L); //全局搜索范围所处的格网
    public static int ENDLEVEL = 14;  //格网搜索的最后一级

    public enum StatisticIndexEnum {
        //面积、长度、个数、表面面积、表面长度
        AREA, LENGTH, COUNT,SURFACEAREA,SURFACELENGTH;
    }

    //12位行政区划编码+4位要素分类编码+8位要素唯一编码
    public static String GetROWCODE(String regionCode,String ccode,String FID){
        return padRightStr(regionCode,12)+padRightStr(ccode,4)+padLeftStr(FID,8);
    }

    /*
     * 根据行值获取分类编码
     */
    public static String GetCategoryCode(String rowcode){
        return rowcode.substring(12, 16);
    }

    public static String GetRegionCode(String rowcode){
        return rowcode.substring(0, 12);
    }


    public static String padLeftStr(String res, int pad) {
        if (pad > 0) {
            while (res.length() < pad) {
                res = "0" + res;
            }
        }
        return res;
    }

    public static String padRightStr(String res, int pad) {
        if (pad > 0) {
            while (res.length() < pad) {
                res = res + "0";
            }
        }
        return res;
    }

    /*
     * 提取记录的属性信息，即去掉数组中后面的五个数据
     */
    public static String getPropertiesStr(String[] values){
        StringBuffer sb = new StringBuffer("");
        if(values.length>=6){
            for(int i=0;i<values.length-7;i++){
                sb.append(values[i]+DEFAULT_INPUT_DELIMITER);
            }
        }
        return sb.toString();
    }

    public static Map<String,String> resolveProperty(String property){
        Map<String,String> properties = new HashMap<String,String>();
        if(property!=null){
            String[] arr = property.split(DEFAULT_INPUT_DELIMITER);
            for(int i=0;i<arr.length;i++){
                if(arr[i].length()>0){
                    String[] tuple = arr[i].split(":");
                    if(tuple.length>=2){
                        properties.put(tuple[0], tuple[1]);
                    }
                }
            }
        }

        return properties;

    }

    /*
     * 获取集合类型，暂支持point、polygon、polyline
     */
    public static Type getGeometryType(String geoType){
        if(geoType.toLowerCase().equals("point")){
            return Geometry.Type.Point;
        }
        if(geoType.toLowerCase().equals("polygon")){
            return Geometry.Type.Polygon;
        }
        if(geoType.toLowerCase().equals("polyline")){
            return Geometry.Type.Polyline;
        }
        return Geometry.Type.Unknown;
    }
    /*
     * 获取矩形的字符串表示，根据xmin,ymin,xmax,ymax形式表示
     */
    public static String GetEnvelopeStr(Envelope env){
        //  System.out.println(Double.toString(env.getLowerLeft().getX())+DEFAULT_INPUT_DELIMITER+Double.toString(env.getLowerLeft().getY())+DEFAULT_INPUT_DELIMITER+Double.toString(env.getUpperRight().getX())+DEFAULT_INPUT_DELIMITER+Double.toString(env.getUpperRight().getY()));
        return Double.toString(env.getLowerLeft().getX())+DEFAULT_INPUT_DELIMITER+Double.toString(env.getLowerLeft().getY())+DEFAULT_INPUT_DELIMITER+Double.toString(env.getUpperRight().getX())+DEFAULT_INPUT_DELIMITER+Double.toString(env.getUpperRight().getY());
    }


    public static Envelope GetEnvelope(String envStr){
        String[] coordinates = envStr.split(DEFAULT_INPUT_DELIMITER);
        double lowerleftX = Double.parseDouble(coordinates[0]);
        double lowerleftY = Double.parseDouble(coordinates[1]);
        double upperrightX = Double.parseDouble(coordinates[2]);
        double upperrightY = Double.parseDouble(coordinates[3]);

        return new Envelope(lowerleftX,lowerleftY,upperrightX,upperrightY);
    }

    /*
     * 获取集合图形的中心点信息
     */
    public static String GetGeometryCenterPointStr(Geometry geo){
        if(geo.getType().equals(Type.Point)){
            return getPointStr((Point)geo);
        }
        if(geo.getType().equals(Type.Polygon)){
            Polygon polygon = (Polygon)geo;

            return getPointStr(polygon.GetInteriorPoint());
        }
        if(geo.getType().equals(Type.Polyline)){
            Polyline line = (Polyline)geo;
            int centerpointindex = line.getPointCount()/2;
            return getPointStr(line.getPoint(centerpointindex));
        }
        return null;
    }

    public static String getPointStr(Point p){
        //  System.out.println(Double.toString(p.getX())+DEFAULT_INPUT_DELIMITER+Double.toString(p.getY()));
        return Double.toString(p.getX())+","+Double.toString(p.getY());
    }

    /*
     * 获取要素的统计指标信息，目前支持数量、面积、长度的统计
     */
    public static List<Tuple2<String,String>> getStatIndexKeyValue(Geometry geo){
        List<Tuple2<String,String>> list = new ArrayList<Tuple2<String,String>>();
        list.add(new Tuple2<String,String>(StatisticIndexEnum.COUNT.toString(),"1"));
        if(geo.getType().equals(Type.Polygon)){
            list.add(new Tuple2<String,String>(StatisticIndexEnum.AREA.toString(),Double.toString(geo.calculateArea2D())));
            list.add(new Tuple2<String,String>(StatisticIndexEnum.LENGTH.toString(),Double.toString(geo.calculateLength2D())));
        }
        if(geo.getType().equals(Type.Polyline)){
            list.add(new Tuple2<String,String>(StatisticIndexEnum.LENGTH.toString(),Double.toString(geo.calculateLength2D())));
        }
        return list;
    }

    /*
     * 将数组记录转换成Put对象，并生成索引Put记录
     */
    public static Tuple2<Put,Put> getRecordPut(String [] values,long ts){
        Put put = getDataRecordPut(values,ts);
        Put indexPut = getSpatialIndexRecordPut(put);

        return new Tuple2<Put,Put>(put,indexPut);
    }

    /*
     * 将数组记录转换成Put对象
     */
    public static Put getDataRecordPut(String [] values,long ts){
        if(values.length<6) {
            return null;
        }
        try{
            //如果用以前的gdb转wkt的工具  就用原来的这一块代码
            String Length = values[values.length-8];
            String Area = values[values.length-7];

            String fid = values[values.length-6];
            String regionCode =  values[values.length-5];
            String caption = values[values.length-4];
            String category = values[values.length-3];
            String geoTypeStr = values[values.length-2];
            String geometryStr = values[values.length-1];

            //用spark转wkt的结果用这一串
//            String regionCode = values[0].substring(0,12);
//            String category = values[0].substring(12,16);
//            String fid =  values[0].substring(values[0].length()-8);
//            String Length = values[11];
//            String Area = values[12];
//            String geometryStr = values[13];
//            String caption = values[3];
//            String geoTypeStr = "MULTIPOLYGON";


            if(category.length()<2){
                category = "9999";    //如果分类码没有找到,则归类"99",表示分类为定义
            }
            if(caption.length()==0){
                System.out.println(values.toString()+"   caption:  "+caption );
                caption = "未命名";
            }

            // Extract each value
            byte [] row = Bytes.toBytes(Utils.GetROWCODE(regionCode,category, fid));
            byte [] CFGeometry = Utils.COLUMNFAMILY_GEOMETRY;
            byte [] CFStatIndex =Utils.COLUMNFAMILY_STATINDEX;

            // Create Put
            Put put = new Put(row);
            //添加要素属性
            byte [] properties = Bytes.toBytes(Utils.getPropertiesStr(values));
            put.add(Utils.COLUMNFAMILY_PROPERTY, Utils.PROPERTY_QUALIFIER,ts, properties);
            //添加标题
            put.add(Utils.COLUMNFAMILY_PROPERTY, Utils.CAPTION_QUALIFIER, ts,Bytes.toBytes(caption));
            //获取外包矩形框与中心点
            Type geometryType = Utils.getGeometryType(geoTypeStr);
            Geometry geo = GeometryEngine.geometryFromWkt(geometryStr, 0,Geometry.Type.Unknown);
            Envelope env = new Envelope();
            geo.queryEnvelope(env);
            byte[] mbr = Bytes.toBytes(Utils.GetEnvelopeStr(env));
            put.add(Utils.COLUMNFAMILY_PROPERTY,Utils.MBR_QUALIFIER,ts,mbr);
            byte[] point = Bytes.toBytes(Utils.GetGeometryCenterPointStr(geo));
            put.add(Utils.COLUMNFAMILY_PROPERTY,Utils.POSITION_QUALIFIER,ts,point);
            //几何类型
            put.add(Utils.COLUMNFAMILY_PROPERTY,Utils.FEATURETYPE_QUALIFIER,ts,Bytes.toBytes(geoTypeStr));
            //添加几何WKT信息
            put.add(Utils.COLUMNFAMILY_GEOMETRY,Utils.GEOMETRY_QUALIFIER,ts,Bytes.toBytes(geometryStr));
            //添加统计指标信息
            List<Tuple2<String,String>> statIndexs = Utils.getStatIndexKeyValue(geo);
            List<String> statValues = new ArrayList<String>();
            statValues.add("1");
            statValues.add(Area);
            statValues.add(Length);
//            statIndexs.add(new Tuple2<String,String>(StatisticIndexEnum.COUNT.toString(),"1"));
//            statIndexs.add(new Tuple2<String,String>(StatisticIndexEnum.AREA.toString(),Area));
//            statIndexs.add(new Tuple2<String,String>(StatisticIndexEnum.LENGTH.toString(),Length));
            for(int i=0;i<statIndexs.size();i++){
//                put.add(Utils.COLUMNFAMILY_STATINDEX,Bytes.toBytes(statIndexs.get(i)._1),ts,Bytes.toBytes(statIndexs.get(i)._2));
                put.add(Utils.COLUMNFAMILY_STATINDEX,Bytes.toBytes(statIndexs.get(i)._1),ts,Bytes.toBytes(statValues.get(i)));
            }
            //生成SpatialIndexPut
//            byte[] indexrow =  Utils.GetIndexRow(env, Utils.UNIVERSEGRID._1(), Utils.ENDLEVEL);
//            Put indexPut = new Put(indexrow);
//            indexPut.add(Bytes.toBytes(category.substring(0, 2)), row,ts, row);
            return put;
        }catch(Exception ex){
            return null;
        }

    }

    /*
     * 根据数据的Put生成空间索引的Put
     */
    public static Put getSpatialIndexRecordPut(Put dataput){
        //生成SpatialIndexPut
        byte[] row = dataput.getRow();
        List<Cell> cells = dataput.get(Utils.COLUMNFAMILY_PROPERTY,Utils.MBR_QUALIFIER);
        String category = GetCategoryCode(Bytes.toString(row));
        if(cells.size()>0){
            Cell cell = cells.get(0);
            String envStr = Bytes.toString(cell.getValueArray(),
                    cell.getValueOffset(), cell.getValueLength());
            Envelope env = Utils.GetEnvelope(envStr);
            byte [] indexrow =  Utils.GetIndexRow(env, Utils.UNIVERSEGRID._1(), Utils.ENDLEVEL);
            Put indexPut = new Put(indexrow);
            indexPut.add(Bytes.toBytes(category.substring(0, 2)), row,cells.get(0).getTimestamp(), row);
            return indexPut;
        }else{
            return null;
        }
    }

    /*
     * 创建空间索引表,按照CC分类组成列簇
     */
    public static void CreateSpatialIndexTable(byte[][] splitsKeys,String referenceTable,Configuration conf) throws IOException{
        //	Configuration conf = HBaseConfiguration.create();
        HBaseHelper hbaseHelpher = HBaseHelper.getHelper(conf);
        String spatialIndexName = GetSpatialIndexTableNameByDataTableName(referenceTable);
        if(hbaseHelpher.existsTable(spatialIndexName)){
            if(HTable.isTableEnabled(spatialIndexName)){
                hbaseHelpher.disableTable(spatialIndexName);
            }
            hbaseHelpher.dropTable(spatialIndexName);
        }
        if(splitsKeys==null){
            hbaseHelpher.createTable(spatialIndexName, CC);
        }
        else
            hbaseHelpher.createTable(spatialIndexName,splitsKeys, CC);
    }




    /*
     * 根据env的范围，寻找包含该env的最小的格网
     */
    public static long GetOptimalGrid(Envelope env,int startLevel,int endLevel){
        int level = startLevel;
        long row = -1;
        long col = -1;
        for(int i=startLevel;i<=endLevel;i++){
            List<Tuple3<Integer,Long,Long>> lst = GetCurrentLevelIntersectGrid(env,i);
            if(lst.size()>1){
                break;
            }
            else if(lst.size()==1)
            {
                level = i;
                row = lst.get(0)._2();
                col = lst.get(0)._3();
            }
            else   //该env不在搜索的范围内，这个应该是不可能的
                return -1;
        }
        if(row==-1||col==-1){ //说明startLevel的级别设置过低，导致没有一个格网能够完全容纳下env
            return -1;
        }
        //以Z曲线编码的方式返回
        return ZCurve.GetZValue(level, row, col);
    }

    /*
     * 获取env与当前层级相交的行列号
     */
    public static List<Tuple3<Integer,Long,Long>> GetCurrentLevelIntersectGrid(Envelope env,int currentLevel){

        double gridSize = 360/(Math.pow(2, currentLevel));
        long startCol = (long) (env.getXMin()/gridSize);
        long endCol = (long) (env.getXMax()/gridSize)+1;
        long startRow = (long) ((180-env.getYMax())/gridSize);
        long endRow =  (long) ((180-env.getYMin())/gridSize)+1;
        List<Tuple3<Integer,Long,Long>> intersectGrid = new ArrayList();
        for(long i=startRow;i<endRow;i++){
            for(long j=startCol;j<endCol;j++){
                intersectGrid.add(new Tuple3<Integer,Long,Long>(currentLevel,i,j));
            }
        }
        //System.out.println("Level:"+currentLevel+" Row:"+startRow+" Col:"+startCol);
        return intersectGrid;
    }

    /*
     * 获取env与当前层级相交的行列号,包含是否格网全包含的信息
     */
    public static List<Tuple2<Tuple3<Integer,Long,Long>,Boolean>> GetCurrentLevelIntersectGrid1(Geometry geo,int currentLevel){
        //System.out.println(Utils.GetEnvelopeStr(env));
        Envelope env = new Envelope();
        geo.queryEnvelope(env);
        double gridSize = 360/(Math.pow(2, currentLevel));
        long startCol = (long) (env.getXMin()/gridSize);
        long endCol = (long) (env.getXMax()/gridSize)+1;
        long startRow = (long) ((180-env.getYMax())/gridSize);
        long endRow =  (long) ((180-env.getYMin())/gridSize)+1;
        List<Tuple2<Tuple3<Integer,Long,Long>,Boolean>> intersectGrid = new ArrayList();
        boolean isContain;
        for(long i=startRow;i<endRow;i++){
            for(long j=startCol;j<endCol;j++){
                if(i!=startRow&&i!=endRow-1&&j!=startCol&&j!=endCol-1){
                    //非边界格网需要进一步判断格网是否与geo有包含关系
                    Envelope grid = GetEnvelopeByGrid(currentLevel,i,j);
                    if(GeometryEngine.contains(geo, grid, SpatialReference.create(4490)))
                    {
                        isContain = true;
                    }else
                        isContain = false;
                }
                else
                    isContain = false;
                Tuple3<Integer,Long,Long> g = new Tuple3<Integer, Long, Long>(currentLevel,i,j);
                intersectGrid.add(new Tuple2<Tuple3<Integer,Long,Long>,Boolean>(g,isContain));
            }
        }
        return intersectGrid;
    }

    public static Envelope GetEnvelopeByGrid(int level,long row,long col){
        double gridSize = 360/(Math.pow(2, level));
        double xMin = gridSize*col;
        double xMax = gridSize*col+gridSize;
        double yMin = 180-gridSize*(row+1);
        double yMax = 180-gridSize*row;
        Envelope env = new Envelope(xMin,yMin,xMax,yMax);
        return env;
    }




    //10.31
    public static List<Tuple2<Tuple3<Integer,Long,Long>,Boolean>> GetIntersectGrid(Tuple3<Integer,Long,Long> currentGrid,Geometry geo,int endLevel){
        Envelope env = new Envelope();
        geo.queryEnvelope(env);
        List<Tuple2<Tuple3<Integer,Long,Long>,Boolean>> gridLst = new ArrayList<Tuple2<Tuple3<Integer,Long,Long>,Boolean>>();
        int currentLevel = currentGrid._1();
//        System.out.println("=======currentlevel: "+currentLevel+"========");
        if(currentLevel>endLevel){  //超过了最大的搜索层级
            return gridLst;
        }
        Envelope currentGridEnv = GetEnvelopeByGrid(currentGrid._1(), currentGrid._2(), currentGrid._3());
//        System.out.println("if contains? "+env.contains(currentGridEnv));
        if(env.contains(currentGridEnv)){
            //先做外包框的包含判断再做Geometry的包含判读是基于如下考虑：Geometry的包含判断相对复杂费时，大部分格网基于Geometry的外包框就可以过滤掉 20151103
//            System.out.println(GeometryEngine.geometryToWkt(geo,0));
            System.out.println("test spatialref"+SpatialReference.create(4490).toString());
            if(GeometryEngine.contains(geo, currentGridEnv, SpatialReference.create(4490))){
                gridLst.add(new Tuple2<Tuple3<Integer,Long,Long>,Boolean>(currentGrid,true));
            }else{
                gridLst.add(new Tuple2<Tuple3<Integer,Long,Long>,Boolean>(currentGrid,false));
                List<Tuple3<Integer,Long,Long>> children = ComputeChildren(currentGrid);
                for(int i=0;i<children.size();i++){
                    gridLst.addAll(GetIntersectGrid(children.get(i),geo,endLevel));
                }
            }
        }
        else if(env.isIntersecting(currentGridEnv)){
            //半包含关系
//            System.out.println(GeometryEngine.geometryToWkt(geo,0));
            gridLst.add(new Tuple2<Tuple3<Integer,Long,Long>,Boolean>(currentGrid,false));
//			//分解当前格网
            List<Tuple3<Integer,Long,Long>> children = ComputeChildren(currentGrid);
            for(int i=0;i<children.size();i++){
                gridLst.addAll(GetIntersectGrid(children.get(i),geo,endLevel));
            }
        }
        return gridLst;
    }

    /*
     * 获取包含该点的格网,格网以Zvalue值表示
     */
    public static List<Long> GetContainGrid(Point p){
        List<Long> gridLst = new ArrayList<Long>();
        for(int i=Utils.UNIVERSEGRID._1();i<=Utils.ENDLEVEL;i++){
            double gridSize = 360/(Math.pow(2, i));
            long col = (long) (p.getX()/gridSize);
            long row = (long) ((180-p.getY())/gridSize);
            gridLst.add(ZCurve.GetZValue(i, row, col));
        }
        return gridLst;
    }

    /*
     * 计算格网的下一级子格网
     */
    private static List<Tuple3<Integer,Long,Long>> ComputeChildren(Tuple3<Integer,Long,Long> parentGrid){
        List<Tuple3<Integer,Long,Long>> children = new ArrayList<Tuple3<Integer,Long,Long>>();
        children.add(new Tuple3<Integer,Long,Long>(parentGrid._1()+1,parentGrid._2()*2,parentGrid._3()*2));
        children.add(new Tuple3<Integer,Long,Long>(parentGrid._1()+1,parentGrid._2()*2+1,parentGrid._3()*2));
        children.add(new Tuple3<Integer,Long,Long>(parentGrid._1()+1,parentGrid._2()*2,parentGrid._3()*2+1));
        children.add(new Tuple3<Integer,Long,Long>(parentGrid._1()+1,parentGrid._2()*2+1,parentGrid._3()*2+1));
        return children;

    }

    /*
     * 获取索引行值
     */
    public static byte[] GetIndexRow(Envelope env,int startLevel,int endLevel){
        long zvalue = GetOptimalGrid(env,startLevel,endLevel);
        if(zvalue==-1){
            return null;
        }
        return Bytes.toBytes(Long.toString(zvalue));
    }

    /*
     * 上传数据，构建索引
     */
    public static void UploadData(String tablename,String filePath,Configuration conf) throws IOException{
        //数据导入
        //HBaseHelper helpher = new HBaseHelper(conf); //11.13
        HTable tbl = new HTable(conf,tablename);
        HTable tblIndex = new HTable(conf,Utils.GetSpatialIndexTableNameByDataTableName(tablename));
        File file = new File(filePath);
        FileReader fr = new FileReader(file);
        InputStreamReader isr = new InputStreamReader(new FileInputStream(file),"UTF-8");
        BufferedReader reader = new BufferedReader(isr);

        String tempString = null;
        int line = 1;

        List<Put> putlst = new ArrayList();
        List<Put> indexputlst = new ArrayList();
        // 一次读入一行，直到读入null为文件结束
        while ((tempString = reader.readLine()) != null) {
            // 显示行号
            System.out.println(line);
            String[] values = tempString.split(Utils.DEFAULT_INPUT_DELIMITER);
            if(values.length>12){
                continue;
            }
            Tuple2<Put,Put> putTuple = Utils.getRecordPut(values, 20151025);
            putlst.add(putTuple._1);
            indexputlst.add(putTuple._2);
            if(putlst.size()==1000){
                tbl.put(putlst);
                tblIndex.put(indexputlst);
                putlst.clear();
                indexputlst.clear();
            }
            line++;
        }
        if(putlst.size()>0){
            tbl.put(putlst);
            tblIndex.put(indexputlst);
            putlst.clear();
            indexputlst.clear();
        }
        reader.close();
    }


    /*
     * 创建要素存储表，splitFilePath为region分割文件路径，tableName为创建表的表明
     */
    public static void CreateFeatureTable(String splitFilePath,String tableName,Configuration conf) throws IOException{
        int keyNum = 2777;
        String[] columnFamilyArr = new String[]{Bytes.toString(Utils.COLUMNFAMILY_PROPERTY),Bytes.toString(Utils.COLUMNFAMILY_GEOMETRY),Bytes.toString(Utils.COLUMNFAMILY_STATINDEX)};//列簇
        byte[][] splitKeys = new byte[keyNum][];

        File file = new File(splitFilePath);
        if(file.exists()&&file.isFile()){
            InputStreamReader read = new InputStreamReader(new FileInputStream(file));
            BufferedReader bufferedReader = new BufferedReader(read);
            String lineTxt = null;
            int index = 0;
            while((lineTxt = bufferedReader.readLine()) != null&&index<keyNum){
                if(lineTxt.length()>0){
                    splitKeys[index++]=Bytes.toBytes(GetROWCODE(lineTxt,"0000","00000000"));
                }
            }

            HBaseHelper hbaseHelpher = HBaseHelper.getHelper(conf);
            if(hbaseHelpher.existsTable(tableName)){
                if(HTable.isTableEnabled(tableName)){
                    hbaseHelpher.disableTable(tableName);
                }
                hbaseHelpher.dropTable(tableName);
            }
            hbaseHelpher.createTable(TableName.valueOf(tableName),5, splitKeys, columnFamilyArr);

        }else{
            System.out.println("没有找到指定的Split文件");
        }
    }


    /*
     * 根据数据表名称获取该数据表的索引表名称
     */
    public static String GetSpatialIndexTableNameByDataTableName(String dataTableName){
        return Utils.SPATIALINDEX_TABLE+"_SPATIAL_"+dataTableName;
    }

    /*
     * 根据索引表名称获取对应的数据表名称
     */
    public static String GetDataTableNameBySpatialIndexTableName(String indexTableName){
        return indexTableName.replaceFirst(Utils.SPATIALINDEX_TABLE+"_SPATIAL_", "");
    }

    /*
     * wkt
     * 数据检查,将结果输出到流
     */
    public static void DataCheck(String directoryPath,Writer writer ) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "GTED");


        BufferedWriter buffWriter = new BufferedWriter(writer);
        String filepath = directoryPath;

        buffWriter.write("start checking");
        buffWriter.newLine();

        File file = new File(filepath);
        String[] filelist = file.list();

        List<Put> putlist = new ArrayList<Put>();

        for(int i=0;i<filelist.length;i++){
            File readfile = new File(filepath+"\\"+filelist[i]);

            InputStreamReader read = new InputStreamReader(new FileInputStream(readfile),Charset.forName("UTF8"));
            BufferedReader bufferedReader = new BufferedReader(read);

            String lineTxt = null;
            int index = 0;
            int error=0;

            buffWriter.write(filepath+"\\"+filelist[i]);
            buffWriter.newLine();
            while((lineTxt = bufferedReader.readLine()) != null){
                if(lineTxt.length()>0){
                    try{
                        //检查能否构造成Put对象
                        String [] values = lineTxt.toString().split(Utils.DEFAULT_INPUT_DELIMITER);
                        Put put = Utils.getDataRecordPut(values, 20151115);
                        if(put == null){
                            buffWriter.write(lineTxt);
                            buffWriter.newLine();
                            error++;
                        }else{
                            //检查分类码 行政区是否合法
                            String category = Utils.GetCategoryCode(Bytes.toString(put.getRow()));
                            int code  = Integer.parseInt(category);

                            //插入数据库
                            putlist.add(put);

                            if(putlist.size()>100){
                                table.put(putlist);
                                putlist.clear();
                            }

                        }


                    }catch(Exception ex){

                        buffWriter.write(lineTxt);
                        buffWriter.newLine();

                    }

                }
            }

            if(putlist.size()>0){
                table.put(putlist);
                putlist.clear();
            }

            bufferedReader.close();
            read.close();
            buffWriter.flush();
        }
        buffWriter.write("check finished!");
        buffWriter.newLine();
        buffWriter.flush();
        buffWriter.close();

    }


    /*
     * 基于lucene构建关键字索引
     */
    public static void KeyWordIndexBuilder(Configuration conf,String tableName,String outPutPath) throws IOException{
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        Directory direcotry = FSDirectory.open(new File(outPutPath).toPath());
        IndexWriter writer = new IndexWriter(direcotry, iwc);

        HTable tbl = new HTable(conf,tableName);
        Scan scan = new Scan();
        Filter captionFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Utils.CAPTION_QUALIFIER));
        scan.setFilter(captionFilter);

        ResultScanner rs =  tbl.getScanner(scan);
        int count=0;
        int count1 = 0;
        int undefine = 0;
        for (Result result : rs) {
            for (Cell cell : result.rawCells()) {
                String rowkey = Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength());
                String categoryCode = Utils.GetCategoryCode(rowkey);
                String categoryKeyWord = CategoryManager.GetQueryKeyWord(categoryCode);
                String caption = Bytes.toString(cell.getValueArray(),
                        cell.getValueOffset(), cell.getValueLength());
                if(caption.equals("未命名")){
                    System.out.println(rowkey+" "+caption);
                }
                if(!(caption.equals("\\")||caption.equals("未命名"))){
                    caption = caption+" "+categoryKeyWord;
                    Document doc = new Document();
                    doc.add(new StringField("ID", rowkey, org.apache.lucene.document.Field.Store.YES));
                    doc.add(new TextField("KEYWORD", caption, org.apache.lucene.document.Field.Store.YES));
                    if(rowkey.contains("1112")){
                        System.out.println(rowkey+" "+caption);
                    }

                    writer.addDocument(doc);
                    count++;
                    count1++;
                    if(count>10000){
                        writer.commit();
                        count = 0;
                    }
                }else{
                    undefine++;
                }

            }
        }
        writer.close();
        System.out.println(count1);
        System.out.println(undefine);



    }

}


