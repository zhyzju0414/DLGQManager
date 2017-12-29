package edu.zju.gis.gncstatistic;

import edu.zju.gis.hbase.tool.ZCurve;
import scala.Tuple3;

import java.io.Serializable;

/**
 * Created by Frank on 2017/6/15.
 */
public class Grid implements Serializable{

    public long Row;  //行号
    public long Col;  //列号
    public long Level;//层级

    public Grid(long row,long col){
        this.Row = row;
        this.Col = col;
        this.Level = 0;
    }

    public Grid(long row,long col,long level){
        this.Row = row;
        this.Col = col;
        this.Level = level;
    }

    public Grid(Tuple3<Long,Long,Long> tuple3){
        this.Row = tuple3._2();
        this.Col = tuple3._3();
        this.Level = tuple3._1();
    }


    /**
     * 获取行，列号的编码
     * @return
     */
    @Override
    public String toString() {
        return Long.toString(ZCurve.GetZValue(Level,Row,Col));
    }


    /**
     * 将行列号编码转换成行、列号
     * @param
     * @return
     */
    public static Grid Parse(String cellRowCode){
       Tuple3<Long,Long,Long> tuple = ZCurve.ZValue2Tile(Long.parseLong(cellRowCode));
        return new Grid(tuple);
    }

    public static void main(String[] args) {
        Tuple3<Long,Long,Long> tuple = ZCurve.ZValue2Tile(Long.parseLong("818262"));
    }

}
