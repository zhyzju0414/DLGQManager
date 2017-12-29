package edu.zju.gis.gncstatistic;

import com.esri.core.geometry.QuadTree;
import edu.zju.gis.hbase.tool.ZCurve;
import edu.zju.gis.spark.Common.CommonSettings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Frank on 2017/6/15.
 */
public class Utils_zhy {


    /**
     * 获取格网的空间文件路径
     *
     * @param grid
     * @return
     */
    public static String GetGeofilePath(Grid grid,String lcraGridPath) {
        return GetGridRootPath(grid,lcraGridPath) + "/" + CommonSettings.GEOFILE_NAME;
    }

    public static String GetGeofilePath(Sheet sheet,String lcraSheetPath){
        String sheetCode = sheet.toString();
        return lcraSheetPath+"/"+sheetCode.substring(0,3)+"/"+sheetCode+"/"+CommonSettings.GEOFILE_NAME;
    }

    /**
     * 获取格网的属性文件路径
     *
     * @param grid
     * @return
     */
    public static String GetPropertyfilePath(Grid grid,String lcraGridPath) {
        return GetGridRootPath(grid,lcraGridPath) + "/" +  CommonSettings.PROPERTY_FILENAME;
    }

    /**
     * 获取格网的四叉树索件文件路径
     *
     * @param grid
     * @return
     */
    public static String GetSpatialIndexfilePath(Grid grid,String lcraGridPath) {
        return GetGridRootPath(grid,lcraGridPath) + "/" + CommonSettings.SPATIAL_INDEX_FILENAME;
    }

    /**
     * 获取格网的根路径
     *
     * @param grid
     * @return
     */
    public static String GetGridRootPath(Grid grid,String lcraGridPath) {
        return lcraGridPath + "/" + grid.Col + "/" + grid.Row;
    }

    /**
     * 获取格网属性RDD
     *
     * @param ctx
     * @param gridList
     * @return
     */
    public static JavaPairRDD<String, Iterable<String>> GetPropertyRDD(JavaSparkContext ctx, List<Grid> gridList,String lcraGridPath) {
        if (gridList.size() < 1) {
            return null;
        }
        JavaPairRDD<String, Iterable<String>> firstRDD =
                ctx.textFile(GetPropertyfilePath(gridList.get(0),lcraGridPath)).mapToPair(new WholePairFunction(gridList.get(0))).groupByKey();

        List<JavaPairRDD<String, Iterable<String>>> listRDD = new ArrayList<JavaPairRDD<String, Iterable<String>>>();
        for (int i = 1; i < gridList.size(); i++) {
            listRDD.add(ctx.textFile(GetPropertyfilePath(gridList.get(i),lcraGridPath)).mapToPair(new WholePairFunction(gridList.get(i))).groupByKey());
        }
        return ctx.union(firstRDD, listRDD);
    }

    /**
     * 功能类似于getSpatialRDD，只是把CC码也一起放到了key里
     *
     * @param ctx
     * @param gridList
     * @return
     */
    public static JavaPairRDD<String, Iterable<String>> GetCCSpatialRDD(JavaSparkContext ctx, List<Grid> gridList, Map<String, Integer> ccFilter,String lcraGridPath) {

        if (gridList.size() < 1) {
            return null;
        }
        int firstGridIndex = -1;
        List<JavaPairRDD<String, Iterable<String>>> listRDD = new ArrayList<JavaPairRDD<String, Iterable<String>>>();

        //遍历每一个乡镇所在的格网
        for (int i = 0; i < gridList.size(); i++) {
            Grid fGrid = gridList.get(i);
            long zCode = Long.valueOf(fGrid.toString());   //获得该格网的zcode
//            System.out.println("格网号："+ fGrid.Col+" "+fGrid.Row);
            if (CountyTest_zhy.listDataFiles.containsKey(zCode)) {
//                System.out.println("包含code："+ fGrid.Col+" "+fGrid.Row);
                if (firstGridIndex == -1) {
                    firstGridIndex = i;
                } else {
                    String path = GetGeofilePath(gridList.get(i),lcraGridPath);
                    System.out.println("lcraGridPath: "+path);
                    listRDD.add(ctx.textFile(GetGeofilePath(gridList.get(i),lcraGridPath)).mapToPair(new WholePairFunctionCC(gridList.get(i), ccFilter)).groupByKey(512));
                }
            }
        }
        if (firstGridIndex == -1) {
            return null;
        } else {
            //这个时候是把刚才第一个加进去的格网加进去
            JavaPairRDD<String, Iterable<String>> firstRDD = ctx.textFile(GetGeofilePath(gridList.get(firstGridIndex),lcraGridPath)).mapToPair(new WholePairFunctionCC(gridList.get(firstGridIndex), ccFilter)).groupByKey(512);
            return ctx.union(firstRDD, listRDD);
        }
    }

    /**
     *
     * @param ctx
     * @param gridList  这一次job下要计算得格网号
     * @param ccFilter   cc码过滤器
     * @param lcraGridPath  lcra格网在hdfs上的路径
     * @return
     */
    public static JavaPairRDD<String, Iterable<String>> GetSpatialRDD_ccfilterd(JavaSparkContext ctx, List<Grid> gridList, Map<String, Integer> ccFilter,String lcraGridPath) {

        if (gridList.size() < 1) {
            return null;
        }
        int firstGridIndex = -1;

        //listRDD为除了第一个格网之外的所有格网下的WKT RDD 的list
        List<JavaPairRDD<String, Iterable<String>>> listRDD = new ArrayList<JavaPairRDD<String, Iterable<String>>>();
        Map<Long, Integer> listDataFiles = new HashMap<Long, Integer>();
        listDataFiles(CommonSettings.DEFAULT_HDFS_PATH, listDataFiles,lcraGridPath);

        //遍历每一个乡镇所在的格网
        for (int i = 0; i < gridList.size(); i++) {
            Grid fGrid = gridList.get(i);
            long zCode = Long.valueOf(fGrid.toString());   //获得该格网的zcode
//            System.out.println("格网号："+ fGrid.Col+" "+fGrid.Row);
            if (listDataFiles.containsKey(zCode)) {
//                System.out.println("包含code："+ fGrid.Col+" "+fGrid.Row);
                if (firstGridIndex == -1) {
                    firstGridIndex = i;
                } else {
                    String path = GetGeofilePath(gridList.get(i),lcraGridPath);
//                    System.out.println("lcraGridPath: "+path);
                    listRDD.add(ctx.textFile(GetGeofilePath(gridList.get(i),lcraGridPath)).mapToPair(new WholePairFunctionCC2(gridList.get(i), ccFilter)).groupByKey(512));
                }
            }
        }
        if (firstGridIndex == -1) {
            return null;
        } else {
            //这个时候是把刚才第一个加进去的格网加进去
            JavaPairRDD<String, Iterable<String>> firstRDD = ctx.textFile(GetGeofilePath(gridList.get(firstGridIndex),lcraGridPath)).mapToPair(new WholePairFunctionCC2(gridList.get(firstGridIndex), ccFilter)).groupByKey(512);
            return ctx.union(firstRDD, listRDD);
        }
    }

    public static JavaPairRDD<String, Iterable<String>> GetSpatialRDD_ccfilterd(JavaSparkContext ctx, List<Sheet> sheetList, Map<String, Integer> ccFilter,String lcraSheetPath,int a) {

        if (sheetList.size() < 1) {
            return null;
        }
        int firstGridIndex = -1;

        //listRDD为除了第一个格网之外的所有格网下的WKT RDD 的list
        List<JavaPairRDD<String, Iterable<String>>> listRDD = new ArrayList<JavaPairRDD<String, Iterable<String>>>();
        Map<String, Integer> listDataFiles = new HashMap<String, Integer>();
        listDataFiles = Utils_zhy.listDataFiles(CommonSettings.DEFAULT_HDFS_PATH,lcraSheetPath,"sheet");

        //遍历每一个乡镇所在的格网
        for (int i = 0; i < sheetList.size(); i++) {
            Sheet fsheet = sheetList.get(i);
            String sheetCode = fsheet.toString();
            System.out.println("fsheet: "+fsheet);
            if (listDataFiles.containsKey(sheetCode)) {
                if (firstGridIndex == -1) {
                    firstGridIndex = i;
                } else {
                    String path = GetGeofilePath(sheetList.get(i),lcraSheetPath);
                    listRDD.add(ctx.textFile(path).mapToPair(new WholePairFunctionCC3(sheetList.get(i), ccFilter)).groupByKey(512));
                }
            }
        }
        if (firstGridIndex == -1) {
            return null;
        } else {
            //这个时候是把刚才第一个加进去的格网加进去
            JavaPairRDD<String, Iterable<String>> firstRDD = ctx.textFile(GetGeofilePath(sheetList.get(firstGridIndex),lcraSheetPath)).mapToPair(new WholePairFunctionCC3(sheetList.get(firstGridIndex), ccFilter)).groupByKey(512);
            return ctx.union(firstRDD, listRDD);
        }
    }


    public static void listDataFiles(String hdfsname, Map<Long, Integer> listDataFiles,String lcraGridPath) {

        if (hdfsname == null || hdfsname.length() == 0) {
            return;
        }
        Configuration config = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsname), config);
            Path path = new Path(lcraGridPath);

            FileStatus[] fss = fs.listStatus(path);
            for (int i = 0; i < fss.length; i++) {
                if (fss[i].isDirectory()) {
                    Path dpath = new Path(fss[i].getPath().toString());
                    FileStatus[] fsss = fs.listStatus(dpath);
                    for (int j = 0; j < fsss.length; j++) {
                        if (fsss[j].isDirectory()) {
                            long row = Long.valueOf(fsss[j].getPath().getName());
                            long col = Long.valueOf(fss[i].getPath().getName());
                            long zCode = ZCurve.GetZValue(0, row, col);
                            listDataFiles.put(zCode, 0);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Map<String, Integer> listDataFiles(String hdfsname,String lcraGridPath,String fileType){
        Map<String, Integer> listDataFiles = new HashMap<String,Integer>();
        if (hdfsname == null || hdfsname.length() == 0) {
            return listDataFiles;
        }
        Configuration config = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsname), config);
            Path path = new Path(lcraGridPath);
            if(fileType.equals("sheet")){
                FileStatus[] fss = fs.listStatus(path);
                for (int i = 0; i < fss.length; i++) {
                    if (fss[i].isDirectory()) {
                        Path dpath = new Path(fss[i].getPath().toString());
                        FileStatus[] fsss = fs.listStatus(dpath);
                        for (int j = 0; j < fsss.length; j++) {
                            if (fsss[j].isDirectory()) {
                                listDataFiles.put(fsss[j].getPath().getName(), 0);
                            }
                        }
                    }
                }
            }
            return listDataFiles;
        } catch (Exception e) {
            e.printStackTrace();
            return listDataFiles;
        }
    }

    /**
     * 读取格网的四叉树对象
     *
     * @param grid
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static QuadTree GetQuadTree(Grid grid,String lcraGridPath) throws IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.addResource(Utils.class.getClassLoader().getResource("hdfs-site.xml").openStream());

        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(new Path(GetSpatialIndexfilePath(grid,lcraGridPath)));
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        QuadTree qt = (QuadTree) objectInputStream.readObject();

        objectInputStream.close();
        inputStream.close();
        return qt;
    }


    static class WholePairFunction implements PairFunction<String, String, String> {

        Grid grid;

        public WholePairFunction(Grid grid) {
            this.grid = grid;
        }

        @Override
        public Tuple2<String, String> call(String s) throws Exception {
            return new Tuple2<String, String>(grid.toString(), s);
        }
    }

    /**
     * 功能类似于WholePairFunction
     * 只是把CC码放进了key里
     */
    static class WholePairFunctionCC implements PairFunction<String, String, String> {

        Grid grid;
        Map<String, Integer> ccFilter;

        public WholePairFunctionCC(Grid grid, Map<String, Integer> ccFilter) {
            this.grid = grid;
            this.ccFilter = ccFilter;
        }

        @Override
        public Tuple2<String, String> call(String s) throws Exception {
            // 将CC码和FID放进数据中
            String cc;
            String fid;
            String s0 = s.split("\t")[0];
            if (s0.length() > 16) {
                cc = s0.substring(12, 16);
                fid = s0.substring(16);
                if (ccFilter.containsKey(cc)||ccFilter.size()==0) {
                    return new Tuple2<String, String>(grid.toString() + "#" + cc + fid, s);
                }else
                {
                    return new Tuple2<String, String>("skip#thisfeature", s);
                }
            } else {
                System.out.println(" ======== THIS PROPERTY OF WKT IS BAD ========");
                System.out.println(s);
            }
            return new Tuple2<String, String>("ERROR" + "#" + "0000", s); //TODO 把这个数据错误处理掉
        }
    }
    static class WholePairFunctionCC2 implements PairFunction<String, String, String> {

        Grid grid;
        Map<String, Integer> ccFilter;
        boolean inverseFlag;

        public WholePairFunctionCC2(Grid grid, Map<String, Integer> ccFilter) {
            this.grid = grid;
            this.ccFilter = ccFilter;
//            this.inverseFlag = inverseFlag;
        }

        @Override
        public Tuple2<String, String> call(String s) throws Exception {
            // 将CC码和FID放进数据中
            String cc;
            String fid;
            String s0 = s.split("\t")[0];
            if (s0.length() > 16) {
                cc = s0.substring(12, 16);
                if (ccFilter.containsKey(cc)||ccFilter.size()==0) {
                    return new Tuple2<String, String>(grid.toString() + "#" + cc, s);
                }else
                {
                    return new Tuple2<String, String>("skip#thisfeature", s);
                }
            } else {
                System.out.println(" ======== THIS PROPERTY OF WKT IS BAD ========");
                System.out.println(s);
            }
            return new Tuple2<String, String>("ERROR" + "#" + "0000", s); //TODO 把这个数据错误处理掉
        }
    }

    static class WholePairFunctionCC3 implements PairFunction<String, String, String> {

        Sheet sheet;
        Map<String, Integer> ccFilter;

        public WholePairFunctionCC3(Sheet sheet, Map<String, Integer> ccFilter) {
            this.sheet = sheet;
            this.ccFilter = ccFilter;
        }

        @Override
        public Tuple2<String, String> call(String s) throws Exception {
            // 将CC码和FID放进数据中
            String[] strValues = s.split("\t");
            String rowkey = strValues[1];
            String cc;
            String wkt = strValues[2];
            if (rowkey.length() > 16) {
                cc = rowkey.substring(12, 16);
                if (ccFilter.containsKey(cc)||ccFilter.size()==0) {
                    String str = rowkey+"\t"+wkt;
                    return new Tuple2<String, String>(sheet.toString() + "#" + cc, str);
                }else
                {
                    return new Tuple2<String, String>("skip#thisfeature", s);
                }
            } else {
                System.out.println(" ======== THIS PROPERTY OF WKT IS BAD ========");
                System.out.println(s);
            }
            return new Tuple2<String, String>("ERROR" + "#" + "0000", s); //TODO 把这个数据错误处理掉
        }
    }

    public static void main(String[] args) throws Exception {
//        System.out.println(GetSpatialIndexfilePath(new Grid(1, 2)));
        Map<String,String> resultList =  new HashMap<String,String>();
        resultList.put("qbz","zz");
        edu.zju.gis.spark.Utils.SaveResultInOracle("app01",resultList);
    }


}
