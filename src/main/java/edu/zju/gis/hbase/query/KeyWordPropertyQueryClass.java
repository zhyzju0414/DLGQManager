package edu.zju.gis.hbase.query;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.zju.gis.hbase.entity.CategoryStatisticInfo;
import edu.zju.gis.hbase.entity.KeyWordQueryObjList;
import edu.zju.gis.hbase.entity.KeyWordQueryObjList.KeyWordQueryObj;
import edu.zju.gis.hbase.query.querycondition.KeyWordPropertyQueryCondition;
import edu.zju.gis.hbase.query.querycondition.PropertyQueryCondition;
import edu.zju.gis.hbase.query.querycondition.QueryCondition;
import edu.zju.gis.hbase.tool.BatchGetServer;
import edu.zju.gis.hbase.tool.BatchScanServer;
import edu.zju.gis.hbase.tool.CategoryManager;
import edu.zju.gis.hbase.tool.HTablePool;
import edu.zju.gis.hbase.tool.HashGetTaskSpliter;
import edu.zju.gis.hbase.tool.IGetTaskSpliter;
import edu.zju.gis.hbase.tool.IResultProcessor;
import edu.zju.gis.hbase.tool.IScanTaskSpliter;
import edu.zju.gis.hbase.tool.ListGetTaskGet;
import edu.zju.gis.hbase.tool.RawResultProcessor;
import edu.zju.gis.hbase.tool.RegionCategorysBasedScanTaskSpliter;
import edu.zju.gis.hbase.tool.RegionManager;
import edu.zju.gis.hbase.tool.Utils;

public class KeyWordPropertyQueryClass implements IFeatureQueryable {

    private HTablePool tablePool;

    private IndexSearcher indexSearcher;  //基于lucene的索引查询对象

    private int maxhitCount = 1000;   //每次关键字搜索返回的最大记录数


    public KeyWordPropertyQueryClass(HTablePool tablePool,IndexSearcher indexSearcher) {
        super();
        this.tablePool = tablePool;
        this.indexSearcher = indexSearcher;
    }

    @Override
    public List<Result> DoQuery(QueryCondition qc,Filter columnFilter) throws Exception {
        // TODO Auto-generated method stub
        if(qc instanceof KeyWordPropertyQueryCondition){
            KeyWordPropertyQueryCondition pqc = (KeyWordPropertyQueryCondition)qc;
            List<String> rowkeyList = GetDataFilter(pqc);
            List<Get> getList = new ArrayList<Get>();
            for(int i=0;i<rowkeyList.size();i++){
                Get get = new Get(Bytes.toBytes(rowkeyList.get(i)));
                get.setFilter(columnFilter);
                getList.add(get);
            }

            IGetTaskSpliter getTaskSpliter = new HashGetTaskSpliter(getList);
            IResultProcessor rp = new RawResultProcessor();
            BatchGetServer getServer = new BatchGetServer<Result>(getTaskSpliter, tablePool, rp);
            List<Result> rs = getServer.ParallelBatchGet();
            return rs;
        }
        throw new Exception("QueryCondition type error,should be KeyWordPropertyQueryCondition");
    }

    public List<String> GetDataFilter(KeyWordPropertyQueryCondition kpqc) throws ParseException, IOException{
        List<String> list = new ArrayList<String>();

        QueryParser parser = new QueryParser("KEYWORD", new StandardAnalyzer());
        parser.setDefaultOperator(Operator.AND);
        Query query = parser.parse(kpqc.getKeyWord());
        TopDocs docs = indexSearcher.search(query, maxhitCount);
        int minhitCount = maxhitCount>docs.totalHits?docs.totalHits:maxhitCount;
        for (int i = 0; i < minhitCount; i++) {
            final int docID = docs.scoreDocs[i].doc;
            final Document doc = indexSearcher.doc(docID);
            String rowKey = doc.get("ID");
            String categoryCode = Utils.GetCategoryCode(rowKey);
            String regionCode =RegionManager.GetStandardRegionCode(Utils.GetRegionCode(rowKey)) ;
            //过滤行政区与分类编码

            if(kpqc.getRegionCode()!=null&&!RegionManager.Contains(kpqc.getRegionCode(), regionCode)){
                continue;
            }
            if(kpqc.getCategoryCode()!=null){
                for(int j=0;j<kpqc.getCategoryCode().length;j++){
                    if(CategoryManager.Contains(kpqc.getCategoryCode()[j], categoryCode)){
                        list.add(rowKey);
                        break;
                    }
                }
            }else{
                list.add(rowKey);
            }
        }
        return list;
    }

    @Override
    public CategoryStatisticInfo DoStatistics(QueryCondition qc,
                                              String statisticType) {
        // TODO Auto-generated method stub
        return null;
    }

    public KeyWordQueryObjList DoKeyWordQuery(QueryCondition qc) throws Exception{
        if(qc instanceof KeyWordPropertyQueryCondition){
            KeyWordPropertyQueryCondition kpqc = (KeyWordPropertyQueryCondition)qc;
            KeyWordQueryObjList list =new  KeyWordQueryObjList();

            QueryParser parser = new QueryParser("KEYWORD", new StandardAnalyzer());
            parser.setDefaultOperator(Operator.OR);
            Query query = parser.parse(kpqc.getKeyWord());
            TopDocs docs = indexSearcher.search(query, maxhitCount);
            int minhitCount = maxhitCount>docs.totalHits?docs.totalHits:maxhitCount;
            for (int i = 0; i < minhitCount; i++) {
                final int docID = docs.scoreDocs[i].doc;
                final Document doc = indexSearcher.doc(docID);
                String rowKey = doc.get("ID");
                String categoryCode = Utils.GetCategoryCode(rowKey);
                //过滤行政区与分类编码
                if(kpqc.getRegionCode()!=null&&!rowKey.startsWith(kpqc.getRegionCode())){
                    continue;
                }
                if(kpqc.getCategoryCode()!=null){
                    for(int j=0;j<kpqc.getCategoryCode().length;j++){
                        String qcCategoryString = CategoryManager.GetShortCategoryName(kpqc.getCategoryCode()[j]);
                        if(categoryCode.startsWith(qcCategoryString)){
                            list.Add(rowKey, doc.get("KEYWORD"));
                            break;
                        }
                    }
                }else{
                    list.Add(rowKey, doc.get("KEYWORD"));
                }
            }
            return list;
        }
        throw new Exception("QueryCondition type error,should be KeyWordPropertyQueryCondition");
    }


    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                    "namenode,datanode1,datanode2");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
        testDoKeyWordQuery();

    }

    public static void testDoKeyWordQuery() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        HTablePool datatablePool = new HTablePool(10, conf, "GNCFA");
        HTablePool indexdatatablePool =  new HTablePool(10, conf, Utils.GetSpatialIndexTableNameByDataTableName("GNCFA"));

        Directory indexdirectory;
        indexdirectory =FSDirectory.open(new File("\\\\10.2.35.7\\share\\HbaseData\\luceneindex_all").toPath());
        IndexReader indexReader = DirectoryReader.open(indexdirectory);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        KeyWordPropertyQueryClass sqc = new KeyWordPropertyQueryClass(datatablePool,indexSearcher);
        //	String region = "33";
        String[] cats = new String[]{"1112"};
        Date d1 = new Date();
        KeyWordPropertyQueryCondition qcon = new KeyWordPropertyQueryCondition(cats, null,null);

        KeyWordQueryObjList lst = sqc.DoKeyWordQuery(qcon);

        for(int i=0;i<lst.OBJLIST.size();i++){
            System.out.println(lst.OBJLIST.get(i));
        }
        System.out.println(lst.OBJLIST.size());


    }


	/*
	 * 根据行政区、分类码以及columnfilter生成过滤条件
	 */
//	private Filter GetFilter(){
//		//暂不考虑用行政区与分类码进行过滤
//		return getColumnfilter();
//	}

    //D:\luceneindex



}
