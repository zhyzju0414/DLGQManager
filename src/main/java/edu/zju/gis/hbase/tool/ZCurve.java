package edu.zju.gis.hbase.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import scala.Tuple2;
import scala.Tuple3;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Envelope2D;


public class ZCurve {

    public static Long[] magic={0x5555555555555555L,0x3333333333333333L,0x0F0F0F0F0F0F0F0FL,
            0x00FF00FF00FF00FFL,0x0000FFFF0000FFFFL,0x00000000FFFFFFFFL,0x03FFFFFFFFFFFFFFL};

    public static long GetZValue(long level,long row,long column){
        row=(row|(row<<16))&magic[4];
        row=(row|(row<<8))&magic[3];
        row=(row|(row<<4))&magic[2];
        row=(row|(row<<2))&magic[1];
        row=(row|(row<<1))&magic[0];

        column=(column|(column<<16))&magic[4];
        column=(column|(column<<8))&magic[3];
        column=(column|(column<<4))&magic[2];
        column=(column|(column<<2))&magic[1];
        column=(column|(column<<1))&magic[0];
        level = level<<58;
        return (row|column<<1)|level;
    }

    public static Tuple3<Long,Long,Long> ZValue2Tile(long zvalue){
        long level = zvalue>>58;
        zvalue = zvalue&magic[6];

        long row = zvalue&magic[0];
        row = (row|row>>1)&magic[1];
        row = (row|row>>2)&magic[2];
        row = (row|row>>4)&magic[3];
        row = (row|row>>8)&magic[4];
        row = (row|row>>16)&magic[5];

        long column = (zvalue>>1)&magic[0];
        column = (column|column>>1)&magic[1];
        column = (column|column>>2)&magic[2];
        column = (column|column>>4)&magic[3];
        column = (column|column>>8)&magic[4];
        column = (column|column>>16)&magic[5];
        return new Tuple3<Long,Long,Long>(level,row,column);
    }

    /*
     * 根据Z编码获取该编码下的Envelope,universe表示全局外包矩形，zvalue表示Z编码，n表示第0级划分系数
     */
    public static Envelope GetEnvelopeByZvalue(Envelope universe,Long zvalue,Long n){
        //计算编码的层级行列号
        Tuple3<Long,Long,Long> lrc = ZValue2Tile(zvalue);
        double splitNum = Math.pow(n, lrc._1());
        double tileWidth = universe.getWidth()/splitNum;
        double tileHeight = universe.getHeight()/splitNum;
        double xmin = universe.getXMin()+lrc._3()*tileWidth;
        double xmax = universe.getXMin()+(lrc._3()+1)*tileWidth;
        double ymin = universe.getYMin()+lrc._2()*tileHeight;
        double ymax = universe.getYMin()+(lrc._2()+1)*tileHeight;
        return new Envelope(xmin, ymin, xmax, ymax);
    }

    /*
     * 计算该ZValue代表的矩形框的下一级矩形框，n表示0级划分系数
     */
    public static List<Long> ComputeChildren(Long n,Long zvalue){
        List<Long> children= new ArrayList<Long>();
        Tuple3<Long,Long,Long> lrc = ZValue2Tile(zvalue);
        long newlevel = lrc._1()+1;
        long startrow = lrc._2()*n;
        long endrow = (lrc._2()+1)*n;
        long startcol = lrc._3()*n;
        long endcol =  (lrc._3()+1)*n;
        for(long rstart=startrow;rstart<endrow;rstart++){
            for(long cstart=startcol;cstart<endcol;cstart++){
                children.add(GetZValue(newlevel,rstart,cstart));
            }
        }
        return children;
    }

    /*
     * 计算当前格网下levelNum级的zvalue的最大值与最小值
     */
    public static Tuple2<Long,Long> GetZvalueRange(Tuple3<Integer, Long, Long> currentGrid,int levelNum){
        int i=levelNum;
        long minzvalue =ZCurve.GetZValue(currentGrid._1()+i, (int)Math.pow(2, levelNum)*currentGrid._2(), (int)Math.pow(2, levelNum)*currentGrid._3());
        long maxRow = currentGrid._2();
        long maxCol = currentGrid._3();
        while(levelNum>0){
            maxRow = 2*maxRow+1;
            maxCol = 2*maxCol+1;
            levelNum--;
        }
        long maxzvalue = ZCurve.GetZValue(currentGrid._1()+i, maxRow, maxCol);
        return new Tuple2<Long,Long>(minzvalue,maxzvalue);
    }

    public static void main(String[] args){

        //"3746994889986513098"
        Tuple3<Long,Long,Long> t = ZCurve.ZValue2Tile(3746994889986513098L);
        Tuple2<Long,Long> l0 = ZCurve.GetZvalueRange(new Tuple3<Integer,Long,Long>(t._1().intValue(),t._2(),t._3()),0);
        System.out.println("0: "+l0._1+" "+l0._2);

        Tuple2<Long,Long> l1 = ZCurve.GetZvalueRange(new Tuple3<Integer,Long,Long>(t._1().intValue(),t._2(),t._3()),1);
        System.out.println("1: "+l1._1+" "+l1._2);

    }
    //
    public static void printEnvelope(Envelope env){
        Tuple3<Long,Long,Long> t = ZValue2Tile(4035225266181007629L);
        System.out.println(t._1()+","+t._2()+","+t._3());
    }

}
