package edu.zju.gis.hbase.tool;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

/*
 *HTable容器 zxw
 */
public class HTablePool {
    private int tableNum;  //初始化table客户端的个数
    private HTable[] tableArr;   //存放table的数组
    private Random random;

    public HTablePool(int tableNum, Configuration conf,String tableName) {
        super();
        this.tableNum = tableNum;
        this.tableArr = new HTable[this.tableNum];
        random = new Random();
        for(int i=0;i<this.tableNum;i++){
            try {
                this.tableArr[i] = new HTable(conf, tableName);
                this.tableArr[i].setScannerCaching(50);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
    public HTable[] getTableArr() {
        return tableArr;
    }

    public HTable GetTableInstance(){
        return tableArr[random.nextInt(tableNum)];
    }

    public void Close(){
        for(int i=0;i<tableArr.length;i++){
            try {
                tableArr[i].close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
