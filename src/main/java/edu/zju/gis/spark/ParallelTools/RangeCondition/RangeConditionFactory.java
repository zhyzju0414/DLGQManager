package edu.zju.gis.spark.ParallelTools.RangeCondition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import static edu.zju.gis.hbase.tool.RegionManager.GetRegionLevel;
import static edu.zju.gis.spark.Utils.readShp2WktList;

/**
 * Created by Zhy on 2017/11/1.
 */
public class RangeConditionFactory {
    public static RangeCondition createRangeCondition(Map<String,String> parameterMap) throws IOException {
        RangeCondition rangeCondition =null;
//        String wktStr = parameterMap.get("wkt");
//        String wktStr = "MULTIPOLYGON (((122.00040645800004 29.79334925700005, 122.00064858900008 29.79344334500007, 122.00070698400009 29.793470368000044, 122.00092991800011 29.793577049000078, 122.00117702700004 29.793701890000076, 122.001138157 29.79376508000007, 122.00097151900002 29.794041449000076, 122.00086921800005 29.79424978400004, 122.00084221700001 29.79431549700007, 122.00080106300004 29.794316681000055, 122.00079182 29.79430310500004, 122.00078732800011 29.79429509800008, 122.00077367400002 29.794289013000025, 122.00075819300001 29.79429438000005, 122.00073332200009 29.794324594000045, 122.00069534500005 29.794408537000034, 122.00060493800004 29.794593258000077, 122.00040908500013 29.795000467000076, 122.00041648700005 29.79501273400007, 122.00041215000009 29.795029565000046, 122.00022071400006 29.79496781400007, 122.000110316 29.79526668400007, 122.00008081800001 29.795398040000066, 122 29.7953760517416, 122 29.79422824522635, 122.00040645800004 29.79334925700005)))";
//        String wktStr = "MULTIPOLYGON (((122.00727010900003 29.784517964000035, 122.00738393300001 29.78462860700006, 122.00710237500004 29.784879484000047, 122.007627725 29.78543237000002, 122.00698414800002 29.785962269000034, 122.00607226400007 29.785014946000047, 122.00645882400009 29.784659846000068, 122.00671925600011 29.784886152000066, 122.0067681260001 29.78492470700007, 122.00727010900003 29.784517964000035)))";
//        String wktStr = "MULTIPOLYGON (((121.249 30.272,121.254 30.108,121.468 30.112,121.461 30.283)))";
//        String[] wktValues = wktStr.split("#");
        List<String> wktList = new ArrayList<String>();

        String inputFormat = parameterMap.get("inputformat");
//        if(inputFormat.toUpperCase().equals("WKT")){
//            //用户上传wkt文件
//            String wktPath = parameterMap.get("shapepath");
//            wktList = Utils.readTxt2WktList();
//            rangeCondition = new PolygonRange(wktList);
//        }
        if(inputFormat.toUpperCase().equals("SHP")){
            //用户上传shp文件
            System.out.println("用户上传自定义数据");
            String shapePath = parameterMap.get("shapepath");
            wktList = readShp2WktList(shapePath);
            rangeCondition = new PolygonRange(wktList);
        }
        else if(inputFormat.toUpperCase().equals("UNIT")){
//            String[] rowkeyArr = ((String)parameterMap.get("shapepath")).split(",");
            //用户直接选择地理单元
            String rowkey = (String)parameterMap.get("shapepath");
            String regionCC = rowkey.substring(12,16);
            Configuration conf = HBaseConfiguration.create();
            HTable hTable = new HTable(conf,"LCRATEST");
            if(regionCC.equals("1112")||regionCC.equals("1114")||regionCC.equals("1115")){
                //如果是省市县，生成属性查询条件
                System.out.println("省市县查询");
                //直接选择省市县查询
//                String regionCodes = parameterMap.get("regioncode");
//                String[] regionValues = regionCodes.split(",");
                String[] regionValues= new String[1];
                String regioncode = rowkey.substring(0,6);
                regionValues[0] = regioncode;
                int regionLevel = GetRegionLevel(regionValues[0]);
                System.out.println("regionLevel="+regionLevel);
                wktList.add(getWktFromHbase(rowkey,hTable));
                rangeCondition = new DistrictRange(regionValues,wktList,regionLevel);
            }else{
                //如果是地理国情要素单元，生成空间查询条件
                System.out.println("自定义多边形查询");
                //自定义多边形查询
                wktList.add(getWktFromHbase(rowkey,hTable));
                rangeCondition = new PolygonRange(wktList);
            }
        }
        System.out.println("==================wktList===================");
        for(String s:wktList){
            System.out.println(s);
        }
        return rangeCondition;
    }

    public static String getWktFromHbase(String rowkey, HTable hTable) throws IOException {
//        List<Get> getArr  = new ArrayList<Get>();
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes("CFGeometry"),Bytes.toBytes("Geometry"));
        Result result = hTable.get(get);
        String wkt = null;
        if(!result.isEmpty()){
            wkt = Bytes.toString(result.getValue(Bytes.toBytes("CFGeometry"),Bytes.toBytes("Geometry")));
        }
        return wkt;
    };

}
