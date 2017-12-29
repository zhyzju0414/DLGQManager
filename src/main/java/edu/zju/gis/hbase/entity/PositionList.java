package edu.zju.gis.hbase.entity;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "PositionList")
public class PositionList {
    private List<Position> positionList;

    private CategoryStatisticInfo statisticInfo; //统计信息

    public PositionList() {

        this.positionList = new ArrayList<Position>();
    }

    public CategoryStatisticInfo getStatisticInfo() {
        return statisticInfo;
    }

    public void setStatisticInfo(CategoryStatisticInfo statisticInfo) {
        this.statisticInfo = statisticInfo;
    }

    public List<Position> getPositionList() {
        return positionList;
    }

    public void setPositionList(List<Position> positionList) {
        this.positionList = positionList;
    }

    public void AddPosition(Position p){
        this.positionList.add(p);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for(int i=0;i<positionList.size();i++){
            sb.append(positionList.get(i)+"\n");
        }
        return "PositionList [positionList=" + sb.toString() + "]";
    }



}
