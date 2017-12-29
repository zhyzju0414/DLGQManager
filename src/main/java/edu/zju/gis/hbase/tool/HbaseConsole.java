package edu.zju.gis.hbase.tool;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.GenericOptionsParser;

public class HbaseConsole {

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.client.keyvalue.maxsize","524288000");
//            conf.set("hbase.zookeeper.quorum",
//                    "namenode,datanode1,datanode2");
//            conf.set("hbase.zookeeper.property.clientPort", "2181");
        System.out.println(conf.get("hbase.zookeeper.quorum"));
        System.out.println(conf.get("hbase.rootdir"));

//		Utils.KeyWordIndexBuilder(conf, "SHENG", "/home/dlgqjc/share/HbaseData/luceneindex");

//        Utils.CreateFeatureTable(HbaseConsole.class.getClassLoader().getResource("regionspliter.txt").getPath(),"LCRATEST",conf);

//        Utils.UploadData("LCRATEST", "C:\\Users\\Administrator\\Desktop\\BOUA_ADD.txt", conf);

//		args = new String[3];
//		args[0]= "-u";
//		args[1]= "GNCFA2";
//		args[2]= "\\\\10.2.35.7\\share\\HbaseData\\BOUA.txt";
//		args[2]="hdfs://gis252.gis.zju:9000/zhy/lcra_wkt/DLG330206.txt";

//		args[0]="-c";
//		args[1]="\\\\10.2.35.7\\share\\zhy\\regionspliter.txt";
//		args[2]="LCRATEST";

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//
		 if(otherArgs.length<1){
			System.out.println("输入参数不足!");
			PrintHelpInfo();
			return;
		}else{
			if(otherArgs[0].toLowerCase().equals("-c")){
				if(otherArgs.length!=3){
					System.out.println("输入参数有误!");
					PrintHelpInfo();
				}else
				{
					//创建数据表与空间索引表
					Utils.CreateFeatureTable(otherArgs[1], otherArgs[2],conf);
					Utils.CreateSpatialIndexTable(null,otherArgs[2],conf);
					System.out.println("创建完成！");
				}
			}else if(otherArgs[0].toLowerCase().equals("-u")){
				if(otherArgs.length!=3){
					System.out.println("输入参数有误!");
					PrintHelpInfo();
				}else
				{
					Utils.UploadData(otherArgs[1], otherArgs[2],conf);
					System.out.println("导入完成！");
				}
			}else if(otherArgs[0].toLowerCase().equals("-k")){
				if(otherArgs.length!=3){
					System.out.println("输入参数有误!");
					PrintHelpInfo();
				}else
				{
					Utils.KeyWordIndexBuilder(conf, otherArgs[1], otherArgs[2]);
					System.out.println("建立索引完成！");
				}
			}
			else{
				System.out.println("没有找到命令："+otherArgs[0]);
				PrintHelpInfo();
			}
		}
    }

    public static void PrintHelpInfo(){
        StringBuffer sb = new StringBuffer();
        sb.append("命令行：\n");
        sb.append("-c：创建一个矢量数据表。\n");
        sb.append("使用说明：-c <splitFilePath> <tableName> \n");
        sb.append("参数：splitFilePath 分区文件路径;tableName 创建表名。\n");

        sb.append("\n");
        sb.append("-u：导入表数据。\n");
        sb.append("使用说明：-u <dataTable> <datafilePath>\n");
        sb.append("参数：dataTable为数据表表名;datafilePath为导入数据的文件路径。\n");


        System.out.println(sb.toString());
    }



}
