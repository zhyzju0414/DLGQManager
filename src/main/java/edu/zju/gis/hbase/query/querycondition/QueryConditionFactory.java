package edu.zju.gis.hbase.query.querycondition;

import java.util.Map;

import edu.zju.gis.hbase.tool.Utils;

/*
 * 查询条件构造工厂
 */
public class QueryConditionFactory {

    //参数名称
    public static String geoWKTParameterName = "GEOWKT";
    public static String categoryCodeParameterName = "CATEGORYCODE";
    public static String keyWordParameterName = "KEYWORD";
    public static String regionCodeParameterName = "REGIONCODE";

    public static String queryPatternParameterName = "STATISTICONLY"; //统计模式参数，如果为true则仅仅只是进行统计，false则为查询统计

    public static String startindexParameterName = "STARTINDEX";   //分页查询的起始位置参数
    public static String lengthParameterName = "LENGTH";           //长度

    public static String xParameterName = "X";
    public static String yParameterName = "Y";

    public static String gncRowKeyParameterName = "GNCROWKEY";   //地理国情要素的rowkey参数

    public static QueryCondition CreateQueryCondition(Map<String,String> parameterMap){
        String[] categoryCode = null;

        if(parameterMap.containsKey(categoryCodeParameterName)){
            categoryCode = parameterMap.get(categoryCodeParameterName).split(",");
        }
        if(parameterMap.containsKey(geoWKTParameterName)){
            //空间查询
            //	String[] wktArr = parameterMap.get(geoWKTParameterName).split(",");
            //暂时只支持一个多边形的空间查询
            String[] wktArr = new String[1];
            wktArr[0] = parameterMap.get(geoWKTParameterName);
            return new SpatialQueryCondition(categoryCode,wktArr);
        }else{ //属性查询，关键字查询
            String regionCode = null;
            String keyWord = null;
            if(parameterMap.containsKey(regionCodeParameterName)){

                regionCode = parameterMap.get(regionCodeParameterName);
            }
            if(parameterMap.containsKey(keyWordParameterName)){
                //基于关键字检索
                keyWord = parameterMap.get(keyWordParameterName);
                if(keyWord.isEmpty()){
                    keyWord = null;
                }
            }
            if(categoryCode!=null&&regionCode!=null&&keyWord==null){
                return new PropertyQueryCondition(categoryCode,regionCode);
            }
            if(categoryCode!=null){
                return new KeyWordPropertyQueryCondition(categoryCode,regionCode,keyWord);
            }
        }
        return null;
    }

    public static boolean StatisticOnly(Map<String,String> parameterMap){
        if(parameterMap.containsKey(queryPatternParameterName)){
            return Boolean.parseBoolean(parameterMap.get(queryPatternParameterName));
        }else{
            return false;
        }
    }


}
