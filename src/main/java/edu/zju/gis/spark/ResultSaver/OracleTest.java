package edu.zju.gis.spark.ResultSaver;

import edu.zju.gis.spark.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dlgq on 2017/8/22.
 */
public class OracleTest {
    public static void main(String[] args) throws IOException {
        args = new String[4];
        args[0] = "41448040-2c58-4427-901f-27fdbe9eef1c";
        args[3] = "lcrafilepath=hdfs://192.168.1.105:9000/zhy/lcra_sheet_cut/H51";
        args[1] = "demfilepath=/home/otherUsers/zhy/dem10";
        args[2] = "percirclenum=5";
        Map parameters = Utils.ParseParameters(args);
        String lcraFilePath = (String) parameters.get("lcrafilepath");
        String demFilePath = (String) parameters.get("demfilepath");
        Integer perCircleNum = Integer.valueOf((String)parameters.get("percirclenum")) ;
        List<Map<String,String>> resultList = new ArrayList<Map<String,String>>();
        Map<String,String> map = new HashMap<String,String>();
        map.put("REGIONCODE","330206");
        map.put("CCCODE","1001");
        map.put("SURFACEAREA","23443");
        resultList.add(map);
        Map<String,String> map2 = new HashMap<String,String>();
        map2.put("REGIONCODE","330206");
        map2.put("CCCODE","1002");
        map2.put("SURFACEAREA","56564");
        resultList.add(map2);
        Utils.SaveResultInOracle("41448040-2c58-4427-901f-27fdbe9eef1c",resultList);
    }
}
