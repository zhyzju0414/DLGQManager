package edu.zju.gis.gncstatistic;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Envelope2D;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Frank on 2017/6/15.
 */
public interface GridArchitectureInterface<T> {


    /**
     * 获取envelope跨越的行列号范围
     * @param envelope
     * @return 结果中包含两个Grid对象，第一个代表起始的行、列号格网，第二个代表终止的行列号格网
     */
    public Tuple2<T,T> GetCellRowRange(Envelope2D envelope);

    /**
     * 获取grid的空间范围
     * @param grid
     * @return
     */
    public Envelope GetSpatialRange(T grid);

}
