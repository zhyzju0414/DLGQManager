package edu.zju.gis.hbase.query.querycondition;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.filter.Filter;

import edu.zju.gis.hbase.tool.CategoryManager;



/*
 * 查询条件类
 */
public abstract class QueryCondition {

    private String[] categoryCode;   //分类编码，4位


    public QueryCondition(String[] categoryCode) {
        super();
        if(categoryCode!=null){
            this.categoryCode = categoryCode;
            for(int i=0;i<this.categoryCode.length;i++){
                this.categoryCode[i] = CategoryManager.getStandCategoryCode(this.categoryCode[i]);
            }
        }
    }


    public String[] getCategoryCode() {
        return categoryCode;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(categoryCode);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        QueryCondition other = (QueryCondition) obj;
        if (!Arrays.equals(categoryCode, other.categoryCode))
            return false;
        return true;
    }

}
