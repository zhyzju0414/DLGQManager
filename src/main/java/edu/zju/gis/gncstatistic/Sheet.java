package edu.zju.gis.gncstatistic;

import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.Map;

import static edu.zju.gis.gncstatistic.SheetStandardImp.CalcSheetCode;

/**
 * Created by xwhsky on 2017/8/8.
 * 图幅类
 */
public class Sheet implements Serializable {
    public String SheetCode; //图幅号  例如：H51G026005
    public int Row; //行号
    public int Col; //列号
    public int Scale; //当前比例尺
    public int ParentRow; //起始行号(1:1000000)
    public int ParentCol; //起始列号(1:100000)

    public Sheet(String sheetCode){
        SheetCode = sheetCode;
        Scale = GetScale(sheetCode);
        Row =  Integer.parseInt(sheetCode.substring(4, 7));
        Col =  Integer.parseInt(sheetCode.substring(7, 10));
        ParentRow =Integer.valueOf(sheetCode.charAt(0)-64);
        ParentCol = Integer.parseInt(sheetCode.substring(1, 3));
    }

    public Sheet(int row,int col,int scale,int parentRow,int parentCol){
        Row = row;
        Col =col;
        Scale =scale;
        ParentRow = parentRow;
        ParentCol = parentCol;

        String sSheetCode = "";
        sSheetCode = String.valueOf((char) (parentRow + 64)).toUpperCase();       //J
        sSheetCode += StringUtils.leftPad(String.valueOf(parentCol), 2, '0');                   //J50
        sSheetCode += SheetStandardImp.ScaledSheetIndexCharMap.get(scale);      //J50D
        sSheetCode += StringUtils.leftPad(String.valueOf(row), 3, '0');                     //J50D008
        sSheetCode += StringUtils.leftPad(String.valueOf(col), 3, '0');                     //J50D008004
        SheetCode = sSheetCode;
    }

    @Override
    public String toString() {
        return SheetCode;
    }

    /**
     * @Description: 获取图幅号比例尺信息
     */
    public final static int GetScale(String sheetCode) {
        int Scale = 0;
        char e = sheetCode.charAt(3);
        for (Map.Entry<Integer, Character> entry : SheetStandardImp.ScaledSheetIndexCharMap.entrySet()) {
            if (entry.getValue().equals(e)) {
                Scale = entry.getKey();
                break;
            }
        }
        return Scale;
    }

}
