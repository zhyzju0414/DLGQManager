package edu.zju.gis.hbase.entity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import scala.Tuple2;
import edu.zju.gis.hbase.tool.CategoryManager;

//分类统计信息
public class CategoryStatisticInfo {

    public StatisticInfo SUMMARY;    //汇总统计信息


    private Map<String,StatisticInfo> CATEGORYSTATISTICINFO;  //分类统计信息

    public List<Tuple2<String,StatisticInfo>> CATEGORYSTATISTICLIST;  //分类统计列表

    public CategoryStatisticInfo() {
        super();
        SUMMARY = new StatisticInfo();
        CATEGORYSTATISTICINFO = new HashMap<String,StatisticInfo>();
    }



    public CategoryStatisticInfo(StatisticInfo sUMMARY,
                                 Map<String, StatisticInfo> cATEGORYSTATISTICINFO) {
        super();
        SUMMARY = sUMMARY;
        CATEGORYSTATISTICINFO = cATEGORYSTATISTICINFO;
    }

    private StatisticInfo getSUMMARY() {
        return SUMMARY;
    }

    private void setSUMMARY(StatisticInfo sUMMARY) {
        SUMMARY = sUMMARY;
    }

    private Map<String,StatisticInfo> getCATEGORYSTATISTICINFO() {
        return CATEGORYSTATISTICINFO;
    }

    private void setCATEGORYSTATISTICINFO(Map<String,StatisticInfo> cATEGORYSTATISTICINFO) {
        CATEGORYSTATISTICINFO = cATEGORYSTATISTICINFO;
    }



    //进行数据分类汇总统计
    public void doClassifiedStatistic(int toplevel){
        List<String> categoryList = new ArrayList<String>();

        for(Entry<String,StatisticInfo> entry:CATEGORYSTATISTICINFO.entrySet()){
            categoryList.add(entry.getKey());
        }

        for(int i=0;i<categoryList.size();i++){
            int currentlevel = CategoryManager.GetCategoryLevel(categoryList.get(i));
            int gap = currentlevel - toplevel;

            if(gap >= 0){
                CATEGORYSTATISTICINFO.get(categoryList.get(i)).AREAPERCENT = CATEGORYSTATISTICINFO.get(categoryList.get(i)).AREA/SUMMARY.AREA;
                CATEGORYSTATISTICINFO.get(categoryList.get(i)).LENGTHPERCENT = CATEGORYSTATISTICINFO.get(categoryList.get(i)).LENGTH/SUMMARY.LENGTH;
                CATEGORYSTATISTICINFO.get(categoryList.get(i)).COUNTPERCENT = (double)CATEGORYSTATISTICINFO.get(categoryList.get(i)).COUNT/(double)SUMMARY.COUNT;
                if(gap>0){
                    //向上一级汇总
                    String parentCategory = CategoryManager.GetParentLevelCode(categoryList.get(i));
                    if(parentCategory.equals(CategoryManager.TOPCC)){
                        continue;
                    }
                    if(!CATEGORYSTATISTICINFO.containsKey(parentCategory)){
                        CATEGORYSTATISTICINFO.put(parentCategory, new StatisticInfo());
                    }

                    CATEGORYSTATISTICINFO.get(parentCategory).AREA += CATEGORYSTATISTICINFO.get(categoryList.get(i)).AREA;
                    CATEGORYSTATISTICINFO.get(parentCategory).AREAPERCENT = CATEGORYSTATISTICINFO.get(parentCategory).AREA/SUMMARY.AREA;

                    CATEGORYSTATISTICINFO.get(parentCategory).LENGTH += CATEGORYSTATISTICINFO.get(categoryList.get(i)).LENGTH;
                    CATEGORYSTATISTICINFO.get(parentCategory).LENGTHPERCENT = CATEGORYSTATISTICINFO.get(parentCategory).LENGTH/SUMMARY.LENGTH;

                    CATEGORYSTATISTICINFO.get(parentCategory).COUNT += CATEGORYSTATISTICINFO.get(categoryList.get(i)).COUNT;
                    CATEGORYSTATISTICINFO.get(parentCategory).COUNTPERCENT = (double)CATEGORYSTATISTICINFO.get(parentCategory).COUNT/(double)SUMMARY.COUNT;

                    if(gap>1){
                        //继续向上汇总
                        String grandparentCategory = CategoryManager.GetParentLevelCode(parentCategory);
                        if(grandparentCategory.equals(CategoryManager.TOPCC)){  //考虑1001这个变态分类
                            continue;
                        }
                        if(!CATEGORYSTATISTICINFO.containsKey(grandparentCategory)){
                            CATEGORYSTATISTICINFO.put(grandparentCategory, new StatisticInfo());
                        }
                        CATEGORYSTATISTICINFO.get(grandparentCategory).AREA += CATEGORYSTATISTICINFO.get(categoryList.get(i)).AREA;
                        CATEGORYSTATISTICINFO.get(grandparentCategory).AREAPERCENT = CATEGORYSTATISTICINFO.get(grandparentCategory).AREA/SUMMARY.AREA;

                        CATEGORYSTATISTICINFO.get(grandparentCategory).LENGTH += CATEGORYSTATISTICINFO.get(categoryList.get(i)).LENGTH;
                        CATEGORYSTATISTICINFO.get(grandparentCategory).LENGTHPERCENT = CATEGORYSTATISTICINFO.get(grandparentCategory).LENGTH/SUMMARY.LENGTH;

                        CATEGORYSTATISTICINFO.get(grandparentCategory).COUNT += CATEGORYSTATISTICINFO.get(categoryList.get(i)).COUNT;
                        CATEGORYSTATISTICINFO.get(grandparentCategory).COUNTPERCENT = (double)CATEGORYSTATISTICINFO.get(grandparentCategory).COUNT/(double)SUMMARY.COUNT;
                    }
                }
            }
        }
        constructStatisticList();
    }


    public void doClassifiedStatistic(){
        List<String> categoryList = new ArrayList<String>();
        int toplevel = 3;
        for(Entry<String,StatisticInfo> entry:CATEGORYSTATISTICINFO.entrySet()){
            int level = CategoryManager.GetCategoryLevel(entry.getKey());
            if(toplevel>level){
                toplevel = level;
            }
        }
        doClassifiedStatistic(toplevel);
    }


    public void doClassifiedStatistic(String topCategoryCode){
        int level = CategoryManager.GetCategoryLevel(topCategoryCode);
        if(level==0){
            level = 1;
        }
        doClassifiedStatistic(level);
    }

    //将map转换成列表
    public void constructStatisticList(){
        if(CATEGORYSTATISTICLIST==null){
            CATEGORYSTATISTICLIST = new ArrayList<Tuple2<String,StatisticInfo>>();
        }
        for(Entry<String,StatisticInfo> entry:CATEGORYSTATISTICINFO.entrySet()){
            CATEGORYSTATISTICLIST.add(new Tuple2<String,StatisticInfo>(entry.getKey(),entry.getValue()));
        }
        Collections.sort(CATEGORYSTATISTICLIST,new Comparator(){

            @Override
            public int compare(Object arg0, Object arg1) {
                // TODO Auto-generated method stub
                return ((Tuple2<String,StatisticInfo>)arg0)._1.compareTo(((Tuple2<String,StatisticInfo>)arg1)._1);
            }

        });
    }


    public  Map<String,StatisticInfo> gotStatisticMap(){  //为了防止你被无端的序列化 ,只能给你叫got了
        return CATEGORYSTATISTICINFO;
    }

    @Override
    public String toString() {

        StringBuffer sb = new StringBuffer("CategoryStatisticInfo [SUMMARY=" + SUMMARY+"]\n");
        for(int i=0;i<CATEGORYSTATISTICLIST.size();i++){
            sb.append(CATEGORYSTATISTICLIST.get(i));
        }
        return sb.toString();
    }
}
