package edu.zju.gis.hbase.tool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;


public class CategoryManager {

    public static String TOPCC = "0000";

    private static Map<String,String> map;


    static
    {
        map = new HashMap<String,String>();
        map.put("0520","低矮房屋建筑区");
        map.put("0521","高密度低矮房屋建筑区");
        map.put("0522","低密度低矮房屋建筑区");
        map.put("0530","废弃房屋建筑区");
        map.put("0540","多层及以上独立房屋建筑");
        map.put("0541","多层独立房屋建筑");
        map.put("0542","中高层独立房屋建筑");
        map.put("0543","高层独立房屋建筑");
        map.put("0544","超高层独立房屋建筑");
        map.put("0550","低矮独立房屋建筑");
        map.put("0600","道路");
        map.put("0610","铁路");
        map.put("0620","公路");
        map.put("0630","城市道路");
        map.put("0640","乡村道路");
        map.put("0700","构筑物");
        map.put("0710","硬化地表");
        map.put("0711","广场");
        map.put("0712","露天体育场");
        map.put("0713","停车场");
        map.put("0714","停机坪与跑道");
        map.put("0715","硬化护坡");
        map.put("0716","场院");
        map.put("0717","露天堆放场");
        map.put("0718","碾压踩踏地表");
        map.put("0719","其他硬化地表");
        map.put("0720","水工设施");
        map.put("0721","堤坝");
        map.put("0722","闸");
        map.put("0723","排灌泵站");
        map.put("0729","其它水工构筑物");
        map.put("0730","交通设施");
        map.put("0731","隧道");
        map.put("0732","桥梁");
        map.put("0733","码头");
        map.put("0734","车渡");
        map.put("0735","高速公路出入口");
        map.put("0736","加油（气）、充电站");
        map.put("0740","城墙");
        map.put("0750","温室、大棚");
        map.put("0760","固化池");
        map.put("0761","游泳池");
        map.put("0762","污水处理池");
        map.put("0763","晒盐池");
        map.put("0769","其他固化池");
        map.put("0770","工业设施");
        map.put("0780","沙障");
        map.put("0790","其他构筑物");
        map.put("0800","人工堆掘地");
        map.put("0810","露天采掘场");
        map.put("0811","露天煤矿采掘场");
        map.put("0812","露天铁矿采掘场");
        map.put("0813","露天铜矿采掘场");
        map.put("0814","露天采石场");
        map.put("0815","露天稀土矿采掘场");
        map.put("0819","其他采掘场");
        map.put("0820","堆放物");
        map.put("0821","尾矿堆放物");
        map.put("0822","垃圾堆放物");
        map.put("0829","其他堆放物");
        map.put("0830","建筑工地");
        map.put("0831","拆迁待建工地");
        map.put("0832","房屋建筑工地");
        map.put("0833","道路建筑工地");
        map.put("0839","其他建筑工地");
        map.put("0890","其他人工堆掘地");
        map.put("0900","荒漠与裸露地表");
        map.put("0910","盐碱地表");
        map.put("0920","泥土地表");
        map.put("0930","沙质地表");
        map.put("0940","砾石地表");
        map.put("0950","岩石地表");
        map.put("1000","水域");
        map.put("1010","河渠");
        map.put("1011","河流");
        map.put("1012","水渠");
        map.put("1001","水面");
        map.put("1031","水库");
        map.put("1032","坑塘");
        map.put("1040","海面");
        map.put("1050","冰川与常年积雪");
        map.put("1051","冰川");
        map.put("1052","常年积雪");
        map.put("1100","地理单元");
        map.put("1110","行政区划与管理单元");
        map.put("1111","国家级行政区");
        map.put("1112","省级行政区");
        map.put("1113","特别行政区");
        map.put("1114","地、市、州级行政区");
        map.put("1115","县级行政区");
        map.put("1116","乡、镇行政区");
        map.put("1117","行政村");
        map.put("1118","城市中心城区");
        map.put("1119","其他特殊行政管理区");
        map.put("1120","社会经济区域单元");
        map.put("1121","主体功能区");
        map.put("1122","开发区、保税区");
        map.put("1123","国有农、林、牧场");
        map.put("1124","自然、文化保护区");
        map.put("1125","自然、文化遗产");
        map.put("1126","风景名胜区、旅游区");
        map.put("1127","森林公园");
        map.put("1128","地质公园");
        map.put("1129","行、蓄、滞洪区");
        map.put("1130","自然地理单元");
        map.put("1131","流域");
        map.put("1132","地形分区");
        map.put("1133","地貌类型单元");
        map.put("1134","湿地保护区");
        map.put("1135","沼泽区");
        map.put("1140","城镇综合功能单元");
        map.put("1141","居住小区");
        map.put("1142","工矿企业");
        map.put("1143","单位院落");
        map.put("1144","休闲娱乐、景区");
        map.put("1145","体育活动场所");
        map.put("1146","名胜古迹");
        map.put("1147","宗教场所");
        map.put("1200","地形");
        map.put("1210","高程信息");
        map.put("1020","湖泊");
        map.put("1030","库塘");
        map.put("1220","坡度信息");
        map.put("1230","坡向信息");
        map.put("0100","耕地");
        map.put("0110","水田");
        map.put("0120","旱地");
        map.put("0200","园地");
        map.put("0210","果园");
        map.put("0211","乔灌果园");
        map.put("0212","藤本果园");
        map.put("0213","草本果园");
        map.put("0220","茶园");
        map.put("0230","桑园");
        map.put("0240","橡胶园");
        map.put("0250","苗圃");
        map.put("0260","花圃");
        map.put("0290","其他园地");
        map.put("0291","其他乔灌园地");
        map.put("0292","其他藤本园地");
        map.put("0293","其他草本园地");
        map.put("0300","林地");
        map.put("0310","乔木林");
        map.put("0311","阔叶林");
        map.put("0312","针叶林");
        map.put("0313","针阔混交林");
        map.put("0320","灌木林");
        map.put("0321","阔叶灌木林");
        map.put("0322","针叶灌木林");
        map.put("0323","针阔混交灌木林");
        map.put("0330","乔灌混合林");
        map.put("0340","竹林");
        map.put("0350","疏林");
        map.put("0360","绿化林地");
        map.put("0370","人工幼林");
        map.put("0380","稀疏灌丛");
        map.put("0400","草地");
        map.put("0410","天然草地");
        map.put("0411","高覆盖度草地");
        map.put("0412","中覆盖度草地");
        map.put("0413","低覆盖度草地");
        map.put("0420","人工草地");
        map.put("0421","牧草地");
        map.put("0422","绿化草地");
        map.put("0423","固沙灌草");
        map.put("0424","护坡灌草");
        map.put("0429","其他人工草地");
        map.put("0500","房屋建筑（区）");
        map.put("0510","多层及以上房屋建筑区");
        map.put("0511","高密度多层及以上房屋建筑区");
        map.put("0512","低密度多层及以上房屋建筑区");
        map.put("0601","路面");
    }

    /*
     * 获取CC码的分类级别,0代表最高级别 ，1代表一级分类，2代表二级分类，3代表三级分类 ,CC码必须4位
     */
    public static int GetCategoryLevel(String ccode){

        if(ccode.length()!=4){
            return -1;
        }
        if(ccode.equals(TOPCC)){
            return 0;
        }
        if(ccode.endsWith("00")){
            return 1;
        }else if(ccode.endsWith("0")){
            return 2;
        }
        return 3;
    }

    /*
     * 根据分类码获取分类的中文名称
     */
    public static String GetCategoryNameByCode(String code){
        if(map.containsKey(code)){
            return map.get(code);
        }
        return null;
    }

    public static String GetShortCategoryName(String ccode){
        int level = GetCategoryLevel(ccode);
        if(level == 0){
            return "";
        }else if(level==1){
            return ccode.substring(0,2);
        }else if(level==2){
            return ccode.substring(0,3);
        }else if(level==3){
            return ccode;
        }
        return null;
    }

    /*
     * 获取当前分类码的子类
     */
    public static List<String> GetSubCategories(String ccode){
        List<String> keyList = new ArrayList<String>();
        for(String key :map.keySet()){
            if(Contains(ccode,key)&&!ccode.equals(key)){
                keyList.add(key);
            }
        }
        return keyList;
    }

    /*
     * 在基于中文检索时的检索关键字
     */
    public static String GetQueryKeyWord(String ccode){
        StringBuffer sb = new StringBuffer();
        String caption = GetCategoryNameByCode(ccode);
        if(caption!=null){
            sb.append(caption);
            List<String> subCategories = GetSubCategories(ccode);

            for(int i=0;i<subCategories.size();i++){
                sb.append(" ");
                sb.append(GetCategoryNameByCode(subCategories.get(i)));
            }
            return sb.toString();
        }

        return null;
    }


    /*
     * 获取上一级分类的编码
     */
    public static String GetParentLevelCode(String ccode){
        int level = GetCategoryLevel(ccode);
        return GetParentLevelCode(ccode,level-1);
    }

    public static String GetParentLevelCode(String ccode,int level){
        if(level==0){
            return TOPCC;
        }else if(level==1){
            return ccode.substring(0,2)+"00";
        }else if(level==2){
            return ccode.substring(0,3)+"0";
        }else
            return ccode;
    }

    public static int GetTopLevel(String[] categoryArr){
        int topLevel = 3;
        for(int i=0;i<categoryArr.length;i++){
            int level = GetCategoryLevel(categoryArr[i]);
            if(level<topLevel){
                topLevel = level;
            }
        }

        return topLevel;
    }


    /*
     * 获取比categoryCode大的分类编码，比如 categoryCode=0200，则返回0300，categoryCode=1000，则返回2000
     */
    public static String getUpperCategory(String categoryCode){
        int offset = categoryCode.length();
        while(categoryCode.charAt(offset-1)=='0'){
            offset--;
        }
        String sub = categoryCode.substring(0,offset);
        if(sub.charAt(0)=='0'){
            sub = sub.replaceFirst("0", "1");
            String upperCategory = Integer.toString(Integer.parseInt(sub.substring(0,offset))+1);
            if(upperCategory.charAt(0)=='2'){
                upperCategory = upperCategory.replaceFirst("2", "1");
            }else{
                upperCategory = upperCategory.replaceFirst("1", "0");
            }
            return Utils.padRightStr(upperCategory, 4);
        }else{
            String upperCategory = Integer.toString(Integer.parseInt(sub.substring(0,offset))+1);
            return Utils.padRightStr(upperCategory, 4);
        }
    }

    /*
     * 获取分类的标准编码，4位
     */
    public static String getStandCategoryCode(String categoryCode){
        if(categoryCode.length()>4){
            return categoryCode.substring(0, 4);
        }else if(categoryCode.length()<4){
            return Utils.padRightStr(categoryCode,4);
        }
        return categoryCode;
    }

	/*
	 * 类别合并，传入一个类别编码的数组，返回类别合并后的数组，如传入 0100,0110,0210，则返回0100,0210，如传入 0100,0110,0120，则返回0100
	 */
//	public static String[] CombineCategory(String[] categories){
//		for(int i=0;i<categories.length;i++){
//			for(int j=i;j<categories.length;j++){
//				if(Contains(categories[i],categories[j])){
//
//				}
//			}
//		}
//		return categories;
//
//	}

    /*
     * 判断category1是否包含category2
     */
    public static boolean Contains(String category1,String category2){
        category1 = GetShortCategoryName(category1);
        category2 = GetShortCategoryName(category2);
        return category2.startsWith(category1);
    }

    public static void main(String[] args)  {

        System.out.println(GetQueryKeyWord("1100"));

    }



}
