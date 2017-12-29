package edu.zju.gis.hbase.tool;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
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
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import scala.Tuple3;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.Geometry.Type;

import edu.zju.gis.hbase.entity.CategoryStatisticInfo;
import edu.zju.gis.hbase.entity.FeatureClassObj;
import edu.zju.gis.hbase.entity.FeatureClassObjList;
import edu.zju.gis.hbase.entity.PositionList;
import edu.zju.gis.hbase.entity.QueryParameter;
import edu.zju.gis.hbase.featurequeryservice.LcraQueryService;



public class HbaseTest {

    //public static String testwkt = "POLYGON ((118.90929521600003 29.506352231999983, 120.90929481800004 31.506357675000004, 119.90917024199996 30.506410829000004, 119.90913190699996 30.506401747999973,119.90905216600004 30.506237769999984, 119.90901918400004 30.506224212999996, 119.90877592799996 30.506326305000016, 119.90874122399998 30.506290418999981,119.90869928400002 30.506102279000004, 119.90887116199997 30.506037714999991, 119.908908215 30.506016597999974, 119.90890995799998 30.505998238000018, 119.908896486 30.50594307199998, 119.90901560299994 30.50589351299999, 119.90902527799995 30.505908394000016, 119.90903982299994 30.505930761000002, 119.90908772499995 30.505916695999986, 119.90909067899997 30.505917466000028, 119.90929521600003 30.506352231999983))";
//	public static String testwkt = "POLYGON ((119.036814273 30.481976271999997, 121.03681419199995 30.481976223000004, 120.03681238599995 30.481975541999986, 120.036810361 30.481975094999996, 120.03678613199998 30.481999040000005, 120.03676299400001 30.482026254999994, 120.03673323500004 30.482031990999985, 120.03670513300006 30.48204202300002, 120.03666545199997 30.482043465999993, 120.03661585099997 30.482042047999983, 120.03656791200001 30.482066405000012, 120.03653651599996 30.482112237000024, 120.03652494999994 30.482133721000025, 120.03645026000004 30.482099372999983, 120.036413294 30.482065407999983, 120.036360672 30.482017059999976, 120.03631931200005 30.481946666, 120.03625864499998 30.481827350999993, 120.03624691200002 30.481757469999991, 120.03621998100005 30.481597050999994, 120.036224769 30.48148424599998, 120.03623282199999 30.481454558999985, 120.03623281099999 30.481424774000004, 120.03623077500004 30.481386230999988, 120.03622267200001 30.481352941000011, 120.03635463199998 30.481334961000016, 120.03654179299997 30.481293745000016, 120.03656153099996 30.481289752000009, 120.03669699 30.481239169999981, 120.03671823900004 30.481230945999982, 120.03676627699997 30.481372022000016, 120.03680270200005 30.481366578000006, 120.03682525099998 30.481517984999982, 120.03680166000004 30.481526004999978, 120.03674379799998 30.481541773999993, 120.036740882 30.481582133000018, 120.03683895200004 30.481881751999993, 120.03685564600005 30.481934098000011, 120.03681641100002 30.481975477999981, 120.036814273 30.481976271999997))";

    public static String testwkt ="POLYGON((119.98138895471195 30.491716427636714,120.0170945211182 30.60020641787109,119.97177591760257 30.522615475488276,119.90860453088382 30.63591198427734,119.87289896447757 30.527421994042964,119.96559610803226 30.410005612207026,119.98138895471195 30.491716427636714))";
    //public static String testwkt = "POLYGON((119.99364645123703 30.587894231353054,119.99707967877609 30.522147923980008,120.0830820286296 30.523521214995633,120.08617193341476 30.60746362832571,119.99261648297531 30.601798802886258,119.99364645123703 30.587894231353054))";

    public static String  geoWKT="POLYGON((119.03435007781708 28.655665829388685,119.0302302047702 28.454478695599622,119.29802195281708 28.448985531537122,119.3042017623874 28.659099056927747,119.03435007781708 28.655665829388685))";
    public static void main(String[] args) throws Exception {
        //testLcraPropertyService();
        //testLcraSpatialService();
        //ReadText();
        testCustomFilter();
        //testLuceneSearch();
    }

//	public static void testStaticInfo(){
//
//		LcraQueryService service = new LcraQueryService("LCRA");
//		QueryParameter qp = new QueryParameter();
//		qp.regionCode="330181";
//		qp.categoryCode="0212";
//
//		PositionList pl= service.GetFeatureList(qp);
//
//		System.out.println( pl.getPositionList().size());
//		System.out.println( pl.getStatisticInfo());
//		Date d2 = new Date();
//
//		System.out.println(d2);
//
//	}
//
//	//success
//	public static void testLcraPropertyService(){
//		LcraQueryService service = new LcraQueryService("LCRA1");
//		QueryParameter qp = new QueryParameter();
//		qp.regionCode="330106";
//		qp.categoryCode="0000";
//		Date d1 = new Date();
//		PositionList pl = service.GetFeatureList(qp);
//		Date d2 = new Date();
//		System.out.println(pl);
//		CategoryStatisticInfo c = pl.getStatisticInfo();
//		for(int i=0;i<c.CATEGORYSTATISTICLIST.size();i++){
//			System.out.println(c.CATEGORYSTATISTICLIST.get(i)._1+" "+c.CATEGORYSTATISTICLIST.get(i)._2);
//		}
//
//		System.out.println(d1);
//		System.out.println(d2);
//	}
//	//success
//	public static void testLcraPropertyByPageService(){
//		LcraQueryService service = new LcraQueryService("LCRA1");
//		QueryParameter qp = new QueryParameter();
//		qp.regionCode="331181";
//		qp.categoryCode="0110";
//		qp.length="10";
//		qp.startIndex="-1";
//		Date d1 = new Date();
//		FeatureClassObjList pl = service.GetFeatureListByPage(qp);
//		for(int i=0;i<pl.getFeatureClassObjList().size();i++){
//			System.out.println(pl.getFeatureClassObjList().get(i));
//		}
//
//		System.out.println(pl.getFeatureClassObjList().size());
//		Date d2 = new Date();
//		System.out.println(d1);
//		System.out.println(d2);
//	}
//	//success
//	public static void testLcraSpatialService(){
//		LcraQueryService service = new LcraQueryService("LCRA1");
//		QueryParameter qp = new QueryParameter();
//		qp.geoWKT=geoWKT;
//		qp.categoryCode="0000";
//		Date d1 = new Date();
//		PositionList pl = service.GetFeatureList(qp);
//		Date d2 = new Date();
////		for(int i=0;i<pl.getPositionList().size();i++){
////			System.out.println(pl.getPositionList().get(i));
////		}
//		CategoryStatisticInfo c = pl.getStatisticInfo();
//		for(int i=0;i<c.CATEGORYSTATISTICLIST.size();i++){
//			System.out.println(c.CATEGORYSTATISTICLIST.get(i)._1+" "+c.CATEGORYSTATISTICLIST.get(i)._2);
//		}
//
//		System.out.println(pl.getPositionList().size());
//
//		System.out.println(d1);
//		System.out.println(d2);
//	}
//
//	public static void testLcraSpatialByPageService(){
//		LcraQueryService service = new LcraQueryService("LCRA1");
//		QueryParameter qp = new QueryParameter();
//		qp.geoWKT=testwkt;
//		qp.categoryCode="0120";
//		qp.length="10";
//		qp.startIndex="-1";
//		Date d1 = new Date();
//		FeatureClassObjList pl = service.GetFeatureListByPage(qp);
//		System.out.println(pl.getFeatureClassObjList().size());
//		Date d2 = new Date();
//		for(int i=0;i<pl.getFeatureClassObjList().size();i++){
//			System.out.println(pl.getFeatureClassObjList().get(i));
//		}
//		System.out.println(d1);
//		System.out.println(d2);
//	}
//
//	public static void testLcraGetFeatureInfo(){
//		LcraQueryService service = new LcraQueryService("LCRA1");
//
//		Date d1 = new Date();
//		FeatureClassObj fco = service.GetFeatureInfo("330106000000011000030107");
//		System.out.println(fco);
//		Date d2 = new Date();
//		System.out.println(d1);
//		System.out.println(d2);
//	}
//
//	public static void testPoingQuery(){
//		LcraQueryService service = new LcraQueryService("LCRA1");
//		Date d1 = new Date();
//		FeatureClassObj fco = service.GetPointQueryFeature(119.61905639593304, 29.82382227718235);
//		System.out.println(fco);
//		Date d2 = new Date();
//		System.out.println(d1);
//		System.out.println(d2);
//	}


    public static void testGetCustomRefinement() throws IOException{

        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helpher = new HBaseHelper(conf);
        HTable t = new HTable(conf,"LCRA");

        Filter refinementFilter =  new CustomFilter(Bytes.toBytes(testwkt));

        Filter f = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("330100000000011000000162")));
        List<Filter> filters = new ArrayList<Filter>();
        filters.add(f);
        filters.add(refinementFilter);
        FilterList lst = new FilterList(filters);
        Scan scan = new Scan();
        scan.setFilter(lst);
        ResultScanner rs = t.getScanner(scan);
        for(Result res:rs){
            helpher.dumpResult(res);
        }

    }


    /**
     * 修改 by zxw 20170601
     * @param arg0
     * @param geo
     * @throws IOException
     */
    public static void filterRowCells(List<Cell> arg0,Geometry geo) throws IOException {
        // TODO Auto-generated method stub
        boolean foundwkt = false;
        boolean foundtype = false;
        String geoType = null;
        String wkt = null;
        for(int i=0;i<arg0.size();i++){
            KeyValue keyValue = KeyValueUtil.ensureKeyValue(arg0.get(i));
//            if(keyValue.matchingColumn(Utils.COLUMNFAMILY_GEOMETRY, Utils.GEOMETRY_QUALIFIER)){
//                foundwkt = true;
//                wkt = Bytes.toString(keyValue.getValueArray(),
//                        keyValue.getValueOffset(), keyValue.getValueLength());
//            }else if(keyValue.matchingColumn(Utils.COLUMNFAMILY_PROPERTY, Utils.FEATURETYPE_QUALIFIER)){
//                geoType = Bytes.toString(keyValue.getValueArray(),
//                        keyValue.getValueOffset(), keyValue.getValueLength());
//                foundtype = true;
//            }
//            if(foundwkt&&foundtype){
//                break;
//            }
        }
        if(foundwkt&&foundtype){
            Type t = Utils.getGeometryType(geoType);
            Geometry geo1 = GeometryEngine.geometryFromWkt(wkt, 0, t);
            if(geo1!=null){
                if(!GeometryEngine.intersect(geo, geo1, SpatialReference.create(4490)).isEmpty()){
                    System .out.println("OK");
                }
            }

        }
    }

    public static void testCustomFilter() throws IOException{
        Configuration conf = HBaseConfiguration.create();

        HBaseHelper helper = HBaseHelper.getHelper(conf);
//        helper.dropTable("testtable");
//        helper.createTable("testtable", "colfam1");
//        System.out.println("Adding rows to table...");
//        helper.fillTable("testtable", 1, 10, 10, 2, true, "colfam1");

        HTable table = new HTable(conf,"GNCFA");
        List<Filter> filters = new ArrayList<Filter>();

       // String testSTrr="POLYGON((116.38111352920532 39.91584062576294,116.38261556625366 39.914681911468506,116.38188600540161 39.91260051727295,116.38104915618896 39.91339445114136,116.38042688369751 39.91440296173096,116.38111352920532 39.91584062576294))";
//        String testSTrr = "POINT(116.49371047250003 40.076982972430514)";
        String testSTrr="POLYGON((116.38111352920532 39.91584062576294,116.38261556625366 39.914681911468506,116.38188600540161 39.91260051727295,116.38104915618896 39.91339445114136,116.38042688369751 39.91440296173096,116.38111352920532 39.91584062576294))";
     //   testSTrr = "POINT(116.49371047250003 40.076982972430514)";
        Filter filter1 = new CustomFilter(Bytes.toBytes(testSTrr));
        filters.add(filter1);
//        String category = "001115发生无法收到尽快发货";
////        String reg = String.format("\\d{%d}%s\\d{%d}\\d{8}", 12,CategoryManager.GetShortCategoryName(category),4-CategoryManager.GetShortCategoryName(category).length());
//        Filter columnFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(category));
//        filters.add(columnFilter);
//
//        Filter filter3 = new CustomFilter(Bytes.toBytes("val-09.01"));
//        filters.add(filter3);

        FilterList filterList = new FilterList(
                FilterList.Operator.MUST_PASS_ALL, filters);

//        Get get = new Get(Bytes.toBytes("110105000000011000031717"));
//        get.setFilter(filter1);
     //   Result res = table.get(get);
   //     System.out.println("get result: "+res.toString());
        Scan scan = new Scan();
        scan.setFilter(filter1);
        ResultScanner scanner = table.getScanner(scan);
        System.out.println("Results of scan:");

        int i=0;
        for (Result result : scanner) {
            i++;
//            for(KeyValue kv : result.raw()){
//                                System.out.println("kv: " + kv. + ", Value: " +
//                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
//                                cell.getValueLength()));
//            }

//            for (Cell cell : result.rawCells()) {
//                System.out.println("Cell: " + cell + ", Value: " +
//                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
//                                cell.getValueLength()));
//            }
        }
        scanner.close();
        System.out.println("length of result:"+String.valueOf(i));
    }


    public static void Scanindex() throws IOException{
        Configuration conf = HBaseConfiguration.create();
        HTable t = new HTable(conf,Utils.GetSpatialIndexTableNameByDataTableName("LCRA"));

        Scan scan = new Scan();
        ResultScanner rs = t.getScanner(scan);
        int i=0;
        for (Result result : rs) {
            long zvalue =Long.parseLong(Bytes.toString(result.getRow())) ;
            Tuple3<Long,Long,Long> t1 = ZCurve.ZValue2Tile(zvalue);
            System.out.println(zvalue);

            if((Bytes.toString(result.getRow())).startsWith("4")){
                i++;
            }

        }
        System.out.println(i);
        rs.close();
    }

    public static void BatchGet() throws IOException{

        Configuration conf = HBaseConfiguration.create();
        HBaseHelper hbaseHelpher = HBaseHelper.getHelper(conf);
        HTable d = new HTable(conf,"LCRA1");
        //HTable t = new HTable(conf,Utils.GetSpatialIndexTableNameByDataTableName("LCRA"));
        Get get = new Get(Bytes.toBytes("330100000000011000000100"));
        Result r = d.get(get);
        for (Cell cell : r.rawCells()) {
            System.out.println(cell.getValueLength());
        }
    }



    //多线程并发Get
    public static void testParallelBatchGet() throws NumberFormatException, IOException{
        Configuration conf = HBaseConfiguration.create();
        HTablePool htp = new HTablePool(5, conf, "LCRA1");
        List<Get> getlist =Getgetlist();
        Date d1 = new Date();
        IGetTaskSpliter hgs = new HashGetTaskSpliter(getlist,3);
        IResultProcessor rp = new RawResultProcessor();
        BatchGetServer bgs = new BatchGetServer<Result>(hgs, htp, rp);
        List<Result> result = bgs.ParallelBatchGet();
        System.out.println(result.size());
        Date d2 = new Date();
        System.out.println(d1);
        System.out.println(d2);

        for(int i=0;i<result.size();i++){
            System.out.println(Bytes.toString(result.get(i).getRow()));
        }
    }


    //多线程并发Get
    public static void testParallelBatchGet1() throws NumberFormatException, IOException{
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf,"LCRA");
        List<Get> getlist =Getgetlist();
        Date d1 = new Date();
        List<Result> result =new ArrayList();
        Result[] rlist = table.get(getlist);
        for(Result re:rlist){
            if(re!=null&&!re.isEmpty()){
                result.add(re);
            }
        }
        System.out.println(result.size());
        Date d2 = new Date();
        System.out.println(d1);
        System.out.println(d2);
    }


    //多线程并发Scan
    public static void testParallelBatchScan() throws IOException{
        Configuration conf = HBaseConfiguration.create();
        HTablePool htp = new HTablePool(5, conf, "LCRA1");
        String reg = String.format("\\d{%d}%s\\d{%d}\\d{8}", 12,"01",2);
        //设置 startrowkey endrowkey
        Date d1 = new Date();
//		Filter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL,
//			      new RegexStringComparator(reg));
        Filter filter1 = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Utils.POSITION_QUALIFIER));
        IScanTaskSpliter hgs = new RegionCategoryBasedScanTaskSpliter("3301","1001",filter1);
        IResultProcessor rp = new RawResultProcessor();
        BatchScanServer bgs = new BatchScanServer<Result>(hgs, htp, rp);
        List<Result> result = bgs.ParallelBatchScan();


        Date d2 = new Date();
        for(int i=0;i<result.size();i++){
            System.out.println(Bytes.toString(result.get(i).getRow()));
        }
        System.out.println(result.size());
        System.out.println(d1);
        System.out.println(d2);

    }


    public static void constructRowkey() throws IOException{
        FileOutputStream out = null;
        out = new FileOutputStream(new File("D:/rowkey.txt"));
        BufferedOutputStream bos = new BufferedOutputStream(out);


        Configuration conf = HBaseConfiguration.create();
        HTable t = new HTable(conf,"LCRA");
        Scan scan = new Scan();
        ResultScanner sc = t.getScanner(scan);
        for(Result r:sc){
            String rowkey =Bytes.toString(r.getRow());
            bos.write((rowkey+"\r\n").getBytes());
        }
        bos.flush();
        bos.close();
        out.close();
        //	testParallelBatchGet();
    }

    public static List<Get> Getgetlist() throws NumberFormatException, IOException{
        File file = new File("D:/rowkey.txt");

        String reg = String.format("\\d{%d}%s\\d{%d}\\d{8}", 12,"011",1);
        //设置 startrowkey endrowkey
        Filter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator(reg));

        List<Get> getlst = new ArrayList<Get>();
        if(file.exists()&&file.isFile()){
            InputStreamReader read = new InputStreamReader(new FileInputStream(file));
            BufferedReader bufferedReader = new BufferedReader(read);
            String lineTxt = null;
            int index = 0;
            while((lineTxt = bufferedReader.readLine()) != null){
                if(lineTxt.length()>0){
                    Get g = new Get(Bytes.toBytes(lineTxt));
                    g.setFilter(filter1);
                    getlst.add(g);
                }
            }

        }
        return getlst;
    }

//	public static void testCoprocessorStatistic(){
//		LcraQueryService service = new LcraQueryService("LCRA1");
//		QueryParameter qp = new QueryParameter();
//		qp.setRegionCode("330000");
//		qp.setCategoryCode("0000");
//		Date d1 = new Date();
//		CategoryStatisticInfo info = service.GetStatisticInfo(qp);
//
//
//		System.out.println("SUMMARY:"+info.SUMMARY);
//		for(int i=0;i<info.CATEGORYSTATISTICLIST.size();i++){
//			System.out.println(info.CATEGORYSTATISTICLIST.get(i)._1+" "+info.CATEGORYSTATISTICLIST.get(i)._2);
//		}
//
//		Date d2 = new Date();
//		System.out.println(d1);
//		System.out.println(d2);
//	}
//
//
//	public static void testByteCode(){
//		LcraQueryService service = new LcraQueryService("GNCFA");
//		QueryParameter qp = new QueryParameter();
//		qp.regionCode="33";
//		qp.categoryCode="1147";
//		Date d1 = new Date();
//		PositionList pl = service.GetFeatureList(qp);
//		Date d2 = new Date();
//		//System.out.println(pl);
//
//		for(int i=0;i<pl.getPositionList().size();i++){
//			System.out.println(pl.getPositionList().get(i).CAPTION);
//		}
//
//		System.out.println(pl.getPositionList().size());
//		System.out.println(d1);
//		System.out.println(d2);
//	}


    public static void testLuceneSearch() throws IOException, ParseException{
        IndexSearcher indexSearcher;
        Directory indexdirectory;
        indexdirectory =FSDirectory.open(new File("D:\\luceneindex").toPath());
        IndexReader indexReader = DirectoryReader.open(indexdirectory);
        indexSearcher = new IndexSearcher(indexReader);


        QueryParser parser = new QueryParser("KEYWORD", new StandardAnalyzer());
        parser.setDefaultOperator(Operator.AND);
        Query query = parser.parse("坪场");
        TopDocs docs = indexSearcher.search(query, 100);

        for (int i = 0; i < 100; i++) {
            final int docID = docs.scoreDocs[i].doc;
            final Document doc = indexSearcher.doc(docID);
            System.out.println(doc.get("ID")+" "+doc.get("KEYWORD"));
        }
        System.out.println(docs.totalHits);

    }


    public static void ReadText() throws IOException{
        File file = new File("C:\\Users\\zxw\\Desktop\\新建文本文档.txt");



        InputStreamReader read = new InputStreamReader(new FileInputStream(file));
        BufferedReader bufferedReader = new BufferedReader(read);
        String lineTxt = null;
        while((lineTxt = bufferedReader.readLine()) != null){
            if(lineTxt.length()>0){
                System.out.println("map.put("+lineTxt+");");
            }
        }
    }
}
