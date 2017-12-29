package edu.zju.gis.gncstatistic;

import com.esri.core.geometry.*;
import edu.zju.gis.hbase.tool.ZCurve;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.crypto.aes.TestAES;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ui.PagedTable;
import scala.Tuple2;


import java.awt.geom.Arc2D;
import java.net.URI;

import java.util.*;

/**
 * Created by HLS on 2017/6/25.
 * 乡镇行政区 + 全国地表覆盖
 * 测试类
 * args说明：
 * 0: 输入乡镇格网数据文件在HDFS的路径
 * 1: 乡镇格网相较于地表覆盖格网大小的倍数（地表覆盖格网为0.5度）
 * 2: 行政区编号的列号
 * 3: 任务提交个数
 */
public class CountyTest_zhy {

    // 根据输入HDFS配置路径参数获取已存在的格网文件列表
    public static Map<Long, Integer> listDataFiles = new HashMap<Long, Integer>();

    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.out.println("！！！ 参数不够 ！！！");
            System.exit(-2);
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("CountyTest");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        Map parameters = edu.zju.gis.spark.Utils.ParseParameters(args);
        Integer xzqCode =Integer.valueOf((String)parameters.get("xzqcode"));
        Integer taskNum = Integer.valueOf((String)parameters.get("tasknum"));
        String lcraGridPath = (String)parameters.get("lcragridpath");
        String countyPath = (String)parameters.get("countypath");
        String hdfsName = (String)parameters.get("hdfsname");
        System.out.println(xzqCode+" "+taskNum+" "+lcraGridPath+" "+countyPath+" "+hdfsName);

        final Broadcast<Integer> xzqCodeBC = ctx.broadcast(xzqCode);
        listDataFiles(hdfsName, listDataFiles,lcraGridPath);

        JavaRDD<String> temp1 = ctx.textFile(countyPath);
        //如果要筛选乡镇 应该把筛选条件作为一个广播变量分发给每个节点  在每个节点进行flatmaptopair的时候就跳过不属于该乡镇的乡镇
        final Map<String, Integer> countyFilter = new HashMap<String, Integer>();
        countyFilter.put("51",1);
        Map<String,Integer> ccFilter = new HashMap<String, Integer>();
//        ccFilter.put("0601",1);
//        ccFilter.put("0411",1);
        final Broadcast<Map<String, Integer>> countyFilterBC = ctx.broadcast(countyFilter);
        final Broadcast<Map<String, Integer>> ccFilterBC = ctx.broadcast(ccFilter);
        System.out.println(ccFilterBC.getValue());
        System.out.println(countyFilterBC.getValue());

        /**
         * 先计算行政区的第二层细格网
         * Key为行政区格网编号
         * value为行政区property和被格网切分以后的行政区geometry
         * zcode  属性值 几何值
         */
        JavaPairRDD<String, Iterable<Tuple2<String, Geometry>>> temp2 = temp1.flatMapToPair(new PairFlatMapFunction<String, String, Tuple2<String, Geometry>>() {
            @Override
            public Iterator<Tuple2<String, Tuple2<String, Geometry>>> call(String s) throws Exception {
                String wkt = null;
                String regionID = null;
                List<Tuple2<String, Tuple2<String, Geometry>>> out = new ArrayList<Tuple2<String, Tuple2<String, Geometry>>>();
                try {
                    String[] propertys = s.split("\t");
//                    regionID = propertys[10];
                    regionID = propertys[0];
//                    System.out.println(regionID.substring(0,2));
                    //如果乡镇过滤器里面包括了这个key 或者 乡镇过滤器为空  则继续
                    if (countyFilterBC.getValue().containsKey(regionID.substring(0,2))|| countyFilterBC.getValue().size()==0) {
                        System.out.println(regionID);
                        wkt = propertys[propertys.length - 1];
                        if ((!wkt.startsWith("POLYGON")) && (!wkt.startsWith("MULTIPOLYGON"))) {
                            System.out.println(" ========= THIS WKT IS NOT RIGHT ========== ");
                            System.out.println(wkt);
                            System.out.println(s);
                            return out.iterator();
                        }
                        LatLonGridArchitecture countyGridArc = (LatLonGridArchitecture) GridArchitectureFactory.GetGridArchitecture(0.2D, "latlon");
                        Geometry countyGeo = GeometryEngine.geometryFromWkt(wkt, WktImportFlags.wktImportDefaults, Geometry.Type.Polygon);
//                    System.out.println(" ========= COUNTY GEO AREA ========== ");
//                    System.out.println(MeasureClass.EllipsoidArea(wkt));
                        Envelope2D countyEnv = new Envelope2D();
                        countyGeo.queryEnvelope2D(countyEnv);
                        Tuple2<Grid, Grid> gridRange = countyGridArc.GetCellRowRange(countyEnv);
                        long startRow = gridRange._1().Row;
                        long endRow = gridRange._2().Row;
                        long startCol = gridRange._1().Col;
                        long endCol = gridRange._2().Col;
                        for (long i = startRow; i <= endRow; i++) {
                            for (long j = startCol; j <= endCol; j++) {
                                Geometry inGrid = GeometryEngine.intersect(countyGridArc.GetSpatialRange(new Grid(i, j)), countyGeo, null);
                                if (inGrid == null || inGrid.isEmpty()) {
                                    continue;
                                }
                                StringBuffer outProperty = new StringBuffer();
                                outProperty.append(propertys[xzqCodeBC.getValue()]);
                                //outProperty.append(propertys[propertys.length-1]);
                                Tuple2<String, Tuple2<String, Geometry>> outCell = new Tuple2<String, Tuple2<String, Geometry>>(Long.toString(ZCurve.GetZValue(0, i, j)), new Tuple2<String, Geometry>(outProperty.toString(), inGrid));
                                out.add(outCell);
                            }
                        }
                    }
                    return out.iterator();
                } catch (Exception e) {
                    e.printStackTrace();
                    return out.iterator();
                }
            }
        }).groupByKey();
//        System.out.println(temp2.collect().size());
        temp2.cache();

        /**
         * 按照均衡格网内细粒度格网的个数组织细粒度格网
         */
        List<String> slimGridKeys = temp2.keys().distinct().sortBy(new Function<String, Object>() {
            @Override
            public Object call(String s) throws Exception {
                return s;
            }
        }, true, 1).collect();

        int perTaskNum = (slimGridKeys.size() / taskNum) + 1;
        int num = 0;
        int circle = 1;
        Map<String, Integer> taskSchema = new HashMap<String, Integer>();
        for (int i = 0; i < slimGridKeys.size(); i++) {
            num++;
            if (num <= perTaskNum) {
                taskSchema.put(slimGridKeys.get(i), circle);
            } else if (circle < taskNum) {
                circle++;
                taskSchema.put(slimGridKeys.get(i), circle);
                num = 1;
            } else {
                // 处理最后一个分片可能会多出来的个数
                taskSchema.put(slimGridKeys.get(i), circle);
            }
        }

        System.out.println(" ========== SLIM GRID KEY NUM ========== ");
        System.out.println(slimGridKeys.size());
        System.out.println(" ========== TASK SCHEMA NUM ========== ");
        System.out.println(taskSchema.size());

        //taskSchemaBC:   格网的zcode  +  在第几次task里面执行
        //为什么要变成广播变量？在后面对所有格网的RDD 赋予 位于第几个task的时候要用到
        final Broadcast<Map<String, Integer>> taskSchemaBC = ctx.broadcast(taskSchema);

        /**
         * 架构行政区粗格网
         * key为循环编号
         * value为行政区细格网
         * 位于第几个task（循环编号）  格网号  格网里面包含的乡镇
         */
        JavaPairRDD<String, Tuple2<String, Iterable<Tuple2<String, Geometry>>>> tempTest = temp2.mapToPair(new PairFunction<Tuple2<String, Iterable<Tuple2<String, Geometry>>>, String, Tuple2<String, Iterable<Tuple2<String, Geometry>>>>() {
            @Override
            public Tuple2<String, Tuple2<String, Iterable<Tuple2<String, Geometry>>>> call(Tuple2<String, Iterable<Tuple2<String, Geometry>>> stringIterableTuple2) throws Exception {
                Tuple2<String, Tuple2<String, Iterable<Tuple2<String, Geometry>>>> out = new Tuple2<String, Tuple2<String, Iterable<Tuple2<String, Geometry>>>>(String.valueOf(taskSchemaBC.getValue().get(stringIterableTuple2._1())), stringIterableTuple2);
                return out;
            }
        });

        temp2.unpersist();
        tempTest.cache();

        Map<String, String> result = new HashMap<String, String>();
        System.out.println(" ====== TASK NUM TOTAL ======");
        System.out.println(taskNum);
        for (int i = 1; i <= taskNum; i++) {
            System.out.println(" ====== TASK " + i + " START ====== ");
            Date start = new Date();
            //List<Tuple2<String, Iterable<Tuple2<String, Geometry>>>> cells = temp3.lookup(key);

            //获得第i个task的细粒度格网以及包含的行政区要素列表
            List<Tuple2<String, Iterable<Tuple2<String, Geometry>>>> cells = tempTest.lookup(String.valueOf(i));
//            Map<String, Iterable<Tuple2<String, Geometry>>> cellmap = new HashMap<String, Iterable<Tuple2<String, Geometry>>>();
//            for (Tuple2<String, Iterable<Tuple2<String, Geometry>>> county : cells) {
//                cellmap.put(county._1(), county._2());
//            }

            List<Grid> grids = new ArrayList<Grid>();

            //grids：第i个task的细粒度格网列表
            for (Tuple2<String, Iterable<Tuple2<String, Geometry>>> cell : cells) {
                grids.add(Grid.Parse(cell._1()));
            }

            JavaPairRDD<String, Iterable<String>> lcraRdd = Utils_zhy.GetCCSpatialRDD(ctx, grids,ccFilterBC.getValue(),lcraGridPath); // 获取含CC码的Key的地类图斑格网内数据
            if (lcraRdd == null) {
                System.out.println(" ===== LCRA为空 ====== " + String.valueOf(i));
                continue;
            }

            JavaPairRDD<String, String> temp4 = lcraRdd.flatMapToPair(new LCRACalMapCC(cells, xzqCode));

            Map<String, String> cellResult = temp4.reduceByKey(new Function2<String, String, String>() {
                @Override
                public String call(String s, String s2) throws Exception {
                    return String.valueOf(Double.valueOf(s) + Double.valueOf(s2));
                }
            }).collectAsMap();

            Set<String> codes = cellResult.keySet();
            for (String code : codes) {
                if (result.containsKey(code)) {
                    double area = Double.valueOf(result.get(code));
                    double cellarea = Double.valueOf(cellResult.get(code));
                    result.put(code, String.valueOf(area + cellarea));
                } else {
                    result.put(code, cellResult.get(code));
                }
            }

            Date end = new Date();
            System.out.println("所需时间为： " + (end.getTime() - start.getTime()) / 1000.00 + " 秒");
        }
        List<Map<String,String>> resultList = new ArrayList<Map<String,String>>();
        // 所有粗粒度格网的结果统计
        for (String code : result.keySet()) {
            String regionCode = code.split("#")[0];
            String ccCode = code.split("#")[1];
            String area = result.get(code);
            System.out.println(regionCode + "," + ccCode + "," + area);
            Map<String,String> map = new HashMap<String,String>();
            map.put("REGIONCODE",regionCode);
            map.put("CCCODE",ccCode);
            map.put("AREA",area);
            resultList.add(map);
        }

        // 关闭 SparkContext
        ctx.close();

        String applicationID = (String) parameters.get("applicationid");
        edu.zju.gis.spark.Utils.SaveResultInOracle(applicationID,resultList);
    }

    // TODO 完善成递归
    public static void listDataFiles(String hdfsname, Map<Long, Integer> listDataFiles,String lcraGridPath ) {

        if (hdfsname == null || hdfsname.length() == 0) {
            return;
        }
        Configuration config = new Configuration();
        try {
            //FileSystem fs = FileSystem.get(new URI(hdfsPath.substring(0,hdfsPath.lastIndexOf("/")+1)),config);
//            FileSystem fs = FileSystem.get(URI.create("hdfs://202.121.180.252:9000/"), config);
            FileSystem fs = FileSystem.get(URI.create(hdfsname), config);
            //Path path = new Path(hdfsPath.substring(0,hdfsPath.lastIndexOf("/")));
            Path path = new Path(lcraGridPath);

            FileStatus[] fss = fs.listStatus(path);
            for (int i = 0; i < fss.length; i++) {
                //List<String> files = new ArrayList<String>();
                // 第一层肯定是文件夹
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
}