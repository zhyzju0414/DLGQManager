package edu.zju.gis.hbase.tool;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import scala.Tuple2;
import scala.Tuple3;

/*
 *
 */
public class PointQueryCondition implements Serializable{

    public PointQueryCondition(double x, double y) {
        super();
        this.p = new Point(x, y);
    }
    private Point p;


    public List<Get> GetPointSpatialFilter(HTable indexTable) throws IOException{
        List<Long> grids = Utils.GetContainGrid(p);
        List<Get> getindexs = new ArrayList<Get>();
        List<Tuple2<Long,Get>> arr = new ArrayList<Tuple2<Long,Get>>();

        for(int i=0;i<grids.size();i++){
            Result res= indexTable.get(new Get(Bytes.toBytes(grids.get(i).toString())));
            List<Get> getdata = new ArrayList<Get>();
            PointContainFilter pcf = new PointContainFilter(p.getX(), p.getY());

            if(!res.isEmpty()){
                for (Cell cell : res.rawCells()) {
                    Get g = new Get(Bytes.copy(cell.getValueArray(),
                            cell.getValueOffset(), cell.getValueLength()));
                    g.setFilter(pcf);
                    getdata.add(g);
                }

            }
            System.out.println(getdata.size());
        }

        return null;
    }


}
