package edu.zju.gis.gncstatistic;

import org.apache.commons.lang.StringUtils;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xwhsky on 2017/8/7.
 * 分幅数据规范操作类
 */
public final class SheetStandardImp {

    //region 分幅规则相关常量

    //起始比例尺
    public final static int StartScale = 1000000;
    /**
     * @Description: 将格网中的Geometry保存成wkt文件，属性数据保存成属性文件，四叉树索引保存成索引文件
     * @Warn: 仅适合中国地区范围。纬度60°~76°采用经差12、纬差4；纬度76°~88°采用经差24、纬差4；其他采用经差6、纬差4
     */
    public final static Map<Integer,Double[]> ScaledGridSLatLonSizeMap = new LinkedHashMap<Integer, Double[]>()
    {
        private Double[] startGridSize = new Double[]{4.0,6.0};
        {
            put(1000000, startGridSize);
            put(500000, new Double[]{startGridSize[0] / 2, startGridSize[1] / 2});
            put(250000, new Double[]{startGridSize[0] / 2 / 2, startGridSize[1] / 2 / 2});
            put(100000, new Double[]{startGridSize[0] / 2 / 2 / 3, startGridSize[1] / 2 / 2 / 3});

            //1:50000比例尺  经差15'' 纬差10'' 10米格网
            put(50000, new Double[]{startGridSize[0] / 2 / 2 / 3 / 2, startGridSize[1] / 2 / 2 / 3 / 2});
            put(25000, new Double[]{startGridSize[0] / 2 / 2 / 3 / 2 / 2, startGridSize[1] / 2 / 2 / 3 / 2 / 2});

            //1:10000比例尺  经差3'45'' 纬差2'30'' 2米格网
            put(10000, new Double[]{startGridSize[0] / 2 / 2 / 3 / 2 / 2 / 2, startGridSize[1] / 2 / 2 / 3 / 2 / 2 / 2});
            put(5000, new Double[]{startGridSize[0] / 2 / 2 / 3 / 2 / 2 / 2 / 2, startGridSize[1] / 2 / 2 / 3 / 2 / 2 / 2 / 2});
        }
    };

    /**
     * @Description: 1：1000000分幅号对应字符
     * 使用LinkedHashMap可以保证插入顺序，HashMap无法做到
     */
    public final static Map<Integer, Character> ScaledSheetIndexCharMap = new LinkedHashMap<Integer, Character>() {
        {
            put(500000, 'B');
            put(250000, 'C');
            put(100000, 'D');
            put(50000, 'E');
            put(25000, 'F');
            put(10000, 'G');
            put(5000, 'H');
        }
    };

    //endregion


    //region 分幅相关方法

    /**
     * @Description: 根据任意比例尺经纬度数据，获取所在图幅号
     */
    public final static String CalcSheetCode(double lat, double lon ,int iScale) {
        double deltaLat = ScaledGridSLatLonSizeMap.get(iScale)[0];
        double deltaLon = ScaledGridSLatLonSizeMap.get(iScale)[1];
        char e = ScaledSheetIndexCharMap.get(iScale);

        int a = (int) Math.floor(lat / 4) + 1;
        int b = (int) Math.floor(lon / 6) + 31;

        int c = (int) (4 / deltaLat) - (int) Math.floor((lat % 4) / deltaLat);
        int d = (int) Math.floor((lon % 6) / deltaLon) + 1;

        String sSheetCode = "";

        sSheetCode = String.valueOf((char) (a + 64)).toUpperCase();       //J
        sSheetCode += StringUtils.leftPad(String.valueOf(b), 2, '0');                   //J50
        sSheetCode += String.valueOf(e).toUpperCase();      //J50D
        sSheetCode += StringUtils.leftPad(String.valueOf(c), 3, '0');                     //J50D008
        sSheetCode += StringUtils.leftPad(String.valueOf(d), 3, '0');                     //J50D008004

        return sSheetCode;
    }

    /**
     * @Description: 根据任意图幅号，获取西南角经纬度坐标
     */
    public final static double[] CalWestSouthPointOfSheetCode(String sheetCode) {
        int a = (int) sheetCode.charAt(0) - 64;
        int b = Integer.parseInt(sheetCode.substring(1, 3));
        int c = Integer.parseInt(sheetCode.substring(4, 7));
        int d = Integer.parseInt(sheetCode.substring(7, 10));
        char e = sheetCode.charAt(3);
        int iScale = Sheet.GetScale(sheetCode);

        double deltaLat = ScaledGridSLatLonSizeMap.get(iScale)[0];
        double deltaLon = ScaledGridSLatLonSizeMap.get(iScale)[1];

        double lon = (b - 31) * 6 + (d - 1) * deltaLon;
        double lat = (a - 1) * 4 + (4 / deltaLat - c) * deltaLat;
        return new double[]{lat, lon};
    }

    /**
     * @Description: 根据小比例尺所在行列号 换算 大比例尺下 包含的行列号元组（从西北角-东南角）
     */
    public final static Tuple2<int[],int[]> ScaleFromSmallToLarge(int[] rowColNum, int smallScale, int largeScale){
        double smallDeltaLat = ScaledGridSLatLonSizeMap.get(smallScale)[0];
        double largeDeltaLat = ScaledGridSLatLonSizeMap.get(largeScale)[0];
        int c = (int)(smallDeltaLat/largeDeltaLat*(rowColNum[0]-1))+1;
        int d = (int)(smallDeltaLat/smallDeltaLat*(rowColNum[1]-1))+1;
        int[] westNorthRowColNum = new int[]{c,d};

        int c2 = (int)(rowColNum[0]*smallDeltaLat/largeDeltaLat);
        int d2 =  (int)(rowColNum[1]*smallDeltaLat/largeDeltaLat);
        int[] eastSouthRowColNum = new int[]{c2,d2};
        return  new Tuple2<>(westNorthRowColNum,eastSouthRowColNum);
    }

    /**
     * @Description: 根据大比例尺所在行列号 换算 小比例尺下 所在行列号
     */
    public final static int[] ScaleFromLargeToSmall(int[] rowColNum,int largeScale,int smallScale){
        double smallDeltaLat = ScaledGridSLatLonSizeMap.get(smallScale)[0];
        double largeDeltaLat = ScaledGridSLatLonSizeMap.get(largeScale)[0];
        int c = (int)Math.ceil(rowColNum[0]/(smallDeltaLat/largeDeltaLat));
        int d = (int)Math.ceil(rowColNum[1]/(smallDeltaLat/largeDeltaLat));
        return new int[]{c,d};
    }

    /**
     * @Description: 获取下一层级子图幅号列表（更精细比例尺）
     */
    public final  static List<Sheet> CalSubLargerSheets(Sheet parentSheet){
        List<Integer> keys= new ArrayList(SheetStandardImp.ScaledSheetIndexCharMap.keySet());
       int scaleIndex =  keys.indexOf(parentSheet.Scale);
        if(scaleIndex<0||scaleIndex==keys.size()-1)
            return null;
        int subLargeScale = keys.get(scaleIndex+1);
        Tuple2<int[],int[]> subScaledSheetRowColNums= ScaleFromSmallToLarge(new int[]{parentSheet.Row,parentSheet.Col},parentSheet.Scale,subLargeScale);

        List<Sheet> sheets = new ArrayList<>();
        int startRowIndex = subScaledSheetRowColNums._1()[0];
        int startColIndex = subScaledSheetRowColNums._1()[1];
        int endRowIndex = subScaledSheetRowColNums._2()[0];
        int endColIndex = subScaledSheetRowColNums._2()[1];
        for(int i=startRowIndex;i<=endRowIndex;i++){
            for(int j=startColIndex;j<=endColIndex;j++){
                Sheet sheet = new Sheet(i,j,subLargeScale,parentSheet.ParentRow,parentSheet.ParentCol);
                sheets.add(sheet);
            }
        }
        return  sheets;
    }

    //endregion

}
