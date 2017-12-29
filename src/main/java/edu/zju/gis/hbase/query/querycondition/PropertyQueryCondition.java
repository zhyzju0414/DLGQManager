package edu.zju.gis.hbase.query.querycondition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticRequest;
import edu.zju.gis.hbase.tool.RegionManager;


/*
 * 基于属性的查询条件，包括行政区、地物分类
 */
public class PropertyQueryCondition extends QueryCondition {

    private String regionCode;   // 行政区代码


    public PropertyQueryCondition(String[] categoryCode, String regionCode) {
        super(categoryCode);
        if(regionCode!=null){
            this.regionCode = RegionManager.GetStandardRegionCode(regionCode);
        }

    }

    public CategoryStatisticRequest GetCategoryStatisticRequest(){
        List<String> cetegoryList = new ArrayList<String>();
        for(int i=0;i<getCategoryCode().length;i++){
            cetegoryList.add(getCategoryCode()[i]);
        }

        return CategoryStatisticRequest.newBuilder().setRegioncode(regionCode).addAllCategorycode(cetegoryList).build();

    }


    public String getRegionCode() {
        return regionCode;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result
                + ((regionCode == null) ? 0 : regionCode.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        PropertyQueryCondition other = (PropertyQueryCondition) obj;

        if (regionCode == null) {
            if (other.regionCode != null)
                return false;
        } else if (!regionCode.equals(other.regionCode))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "PropertyQueryCondition [regionCode=" + regionCode
                + ", getCategoryCode()=" + Arrays.toString(getCategoryCode())
                + "]";
    }





}
