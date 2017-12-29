package edu.zju.gis.hbase.statistic;

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import edu.zju.gis.hbase.entity.CategoryStatisticInfo;
import edu.zju.gis.hbase.entity.Position;
import edu.zju.gis.hbase.entity.StatisticInfo;
import edu.zju.gis.hbase.tool.CategoryManager;
import edu.zju.gis.hbase.tool.Utils;
import edu.zju.gis.hbase.tool.Utils.StatisticIndexEnum;

/*
 * 汇总统计类
 */
public class SummaryQueryStatisticClass extends QueryStatisticClass {



    @Override
    public CategoryStatisticInfo DoStatistic(List<Result> results,String[] categoryCodeArr) {
        // TODO Auto-generated method stub
        CategoryStatisticInfo statisticInfo= new CategoryStatisticInfo();
        int level = 3;
        for(int i=0;i<categoryCodeArr.length;i++){
            if(CategoryManager.GetCategoryLevel(categoryCodeArr[i])<level){
                level = CategoryManager.GetCategoryLevel(categoryCodeArr[i]);
            }
        }


        for(Result res:results){
            if(!res.isEmpty()){
                statisticInfo.SUMMARY.COUNT++;
                String cc = Utils.GetCategoryCode(Bytes.toString(res.getRow()));

                if(!statisticInfo.gotStatisticMap().containsKey(cc)){
                    statisticInfo.gotStatisticMap().put(cc, new StatisticInfo());
                }
                statisticInfo.gotStatisticMap().get(cc).COUNT++;
                for (Cell cell : res.rawCells()) {
                    byte[] qualifier = cell.getQualifier();
                    if(Bytes.equals(qualifier,Bytes.toBytes(StatisticIndexEnum.AREA.toString()))){
                        double area = Double.parseDouble(Bytes.toString(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength()));
                        statisticInfo.SUMMARY.AREA+=area;
                        statisticInfo.gotStatisticMap().get(cc).AREA+=area;
                    }else if(Bytes.equals(qualifier,Bytes.toBytes(StatisticIndexEnum.LENGTH.toString()))){
                        double length = Double.parseDouble(Bytes.toString(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength()));
                        statisticInfo.SUMMARY.LENGTH+=length;
                        statisticInfo.gotStatisticMap().get(cc).LENGTH+=length;
                    }
                }
            }
        }
        statisticInfo.doClassifiedStatistic(level);
        return statisticInfo;
    }

}
