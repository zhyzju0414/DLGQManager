package edu.zju.gis.hbase.query.querycondition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

import edu.zju.gis.hbase.tool.CategoryManager;


/*
 * 基于关键字、行政区、分类信息的属性查询
 */
public class KeyWordPropertyQueryCondition extends PropertyQueryCondition {

    private String keyWord;    //查询关键字


    public KeyWordPropertyQueryCondition(String[] categoryCode,
                                         String regionCode, String keyWord) {
        super(categoryCode, regionCode);
        // TODO Auto-generated constructor stub
        this.keyWord = keyWord;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((keyWord == null) ? 0 : keyWord.hashCode());
        return result;
    }

    /*
     * 若关键字为空，则按类别关键字检索
     */
    public String getKeyWord() {
        if(keyWord==null&&this.getCategoryCode()!=null){
            String keywords="";
            for(int i=0;i<this.getCategoryCode().length;i++){
                keywords = keywords+" "+CategoryManager.GetQueryKeyWord(this.getCategoryCode()[i]);
            }
            return keywords;
        }
        return keyWord;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        KeyWordPropertyQueryCondition other = (KeyWordPropertyQueryCondition) obj;
        if (keyWord == null) {
            if (other.keyWord != null)
                return false;
        } else if (!keyWord.equals(other.keyWord))
            return false;
        return true;
    }


    @Override
    public String toString() {
        return "KeyWordPropertyQueryCondition [keyWord=" + keyWord
                + ", getRegionCode()=" + getRegionCode()
                + ", getCategoryCode()=" + Arrays.toString(getCategoryCode())
                + "]";
    }



}
