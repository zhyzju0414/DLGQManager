package edu.zju.gis.hbase.query;

import java.util.Map;
import java.util.concurrent.Callable;

/*
 * 执行hbase协处理器远程调用的类
 */
public abstract class RpcCallClass<T,R>  implements Callable,Cloneable  {

    public abstract void initializRequest(R request);

    @Override
    public Map<byte[],T> call() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        // TODO Auto-generated method stub
        return super.clone();
    }


}
