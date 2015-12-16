package com.intel.distml.platform;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Model;
import com.intel.distml.util.*;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.InputStream;

/**
 * Created by yunlong on 6/18/15.
 */
public class DataSerializer {

    Kryo kryo;
    Output buf;

    public DataSerializer(int bufferSize, int maxBufferSize) {
        kryo = getKryo();
        buf = new Output(bufferSize, maxBufferSize);
    }

    byte[] serialize(Object obj) {
        try {
            buf.clear();
            kryo.writeClassAndObject(buf, obj);
            return buf.toBytes();
        }
        catch (Exception e) {
            System.out.println("error: pos=" + buf.position() + ", total=" + buf.total());
            e.printStackTrace();
        }
        return null;
    }

    Object deserialize(InputStream is) {

        try {
            Input input = new Input(is);
            Object obj = kryo.readClassAndObject(input);
            input.close();
            return obj;
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private Kryo getKryo() {

        MapReferenceResolver referenceResolver = new MapReferenceResolver();
        Kryo kryo =  new Kryo(new DefaultClassResolver(), referenceResolver);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());

        kryo.setReferences(true);
        kryo.setRegistrationRequired(true);

        kryo.register(int[][].class);
        kryo.register(int[].class);
        kryo.register(float[].class);
        kryo.register(Integer[].class);
        kryo.register(Integer[][].class);
        kryo.register(Float[].class);
        kryo.register(Double[].class);
        kryo.register(java.util.LinkedList.class);
        kryo.register(java.util.HashSet.class);
        kryo.register(java.util.HashMap.class);

        kryo.register(scala.collection.mutable.HashMap.class);


        kryo.register(float[][].class);
        kryo.register(float[][][].class);
        kryo.register(float[][][][].class);
        kryo.register(Float[][].class);

        kryo.register(Model.class);
        kryo.register(DMatrix.class);

        kryo.register(KeyCollection.ALL_KEYS.class);
        kryo.register(KeyCollection.EMPTY_KEYS.class);
        kryo.register(KeyList.class);
        kryo.register(KeyHash.class);
        kryo.register(KeyRange.class);

        kryo.register(DataBusProtocol.FetchRawDataRequest.class);
        kryo.register(DataBusProtocol.PushUpdateRequest.class);
        kryo.register(DataBusProtocol.PushUpdateResponse.class);
        kryo.register(DataBusProtocol.PushDataResponse.class);

        kryo.register(DoubleArray.class);
        kryo.register(IntArray.class);
        kryo.register(IntMatrix.class);

        return kryo;
    }
}
