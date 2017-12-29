package edu.zju.gis.spark.ResultSaver;

/**
 * Created by dlgq on 2017/8/16.
 */
public abstract interface IResultSaver
{
    public abstract boolean SaveResult(String paramString1, String paramString2);
}
