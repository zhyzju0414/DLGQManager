package edu.zju.gis.hbase.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;

import edu.zju.gis.hbase.tool.CategoryManager;
import edu.zju.gis.hbase.tool.Utils;

/*
 * 查询过滤管理器
 */
public class QueryFilterManager {

    /*
     *获取要素的位置列、统计信息列、标题列的过滤器
     */
    public static Filter GetPositionListFilter(){

        //获取位置列 统计信息列 标题列信息
        List<Filter> columnFilters = new ArrayList<Filter>();
        columnFilters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Utils.POSITION_QUALIFIER)));
        columnFilters.add(new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Utils.COLUMNFAMILY_STATINDEX)));
        columnFilters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Utils.CAPTION_QUALIFIER)));
        FilterList columnFiltersList = new FilterList(Operator.MUST_PASS_ONE,columnFilters);
        return columnFiltersList;
    }

    /*
     * 获取要素详情的时候使用的过滤器，过滤掉CFGeometry
     */
    public static Filter GetDetailFeatureFilter(){
        return new FamilyFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Utils.COLUMNFAMILY_GEOMETRY));
    }

    /*
     * 用于过滤空间索引中，当前类别索引位置列
     */
    public static Filter GetSpatialIndexCategoryFilter(String category){
        String parentcategory = category.substring(0, 2);
        if(!parentcategory.equals("00")){
            Filter columnfamilyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes(parentcategory)));
            List<Filter> filterList = new ArrayList<Filter>();
            filterList.add(columnfamilyFilter);
            //根据CC码分类层级进一步判断是否需要进行列过滤
            if(CategoryManager.GetCategoryLevel(category)>1){
                String reg = String.format("\\d{%d}%s\\d{%d}\\d{8}", 12,CategoryManager.GetShortCategoryName(category),4-CategoryManager.GetShortCategoryName(category).length());
                Filter columnFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(reg));
//                System.out.println("reg = "+reg);
//                Filter columnFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL)
                filterList.add(columnFilter);
            }
            return new FilterList(Operator.MUST_PASS_ALL,filterList);
        }
        return null;

    }

}
