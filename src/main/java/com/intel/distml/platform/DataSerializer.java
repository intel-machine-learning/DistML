package com.intel.distml.platform;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.esotericsoftware.minlog.Log;
import com.intel.distml.model.sparselr.SparseWeights;
import com.intel.distml.model.sparselr.WeightItem;
import com.intel.distml.model.word2vec.WordVectorUpdate;
import com.intel.distml.model.word2vec.WordVectorWithAlpha;
import com.intel.distml.util.*;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.InputStream;
import java.io.OutputStream;

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
            //os.write(buf.toBytes());
            //os.close();
//            Output output = new Output(os);
//            kryo.writeClassAndObject(output, obj);
//            output.close();;
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

        kryo.register(int[].class);
        kryo.register(float[].class);
        kryo.register(Float[].class);
        kryo.register(java.util.LinkedList.class);
        kryo.register(java.util.HashSet.class);
        kryo.register(java.util.HashMap.class);

        kryo.register(KeyCollection.ALL_KEYS.class);
        kryo.register(KeyList.class);
        kryo.register(KeyHash.class);
        kryo.register(KeyRange.class);
        kryo.register(GeneralArray.class);

        kryo.register(DataBusProtocol.PartialDataRequest.class);
        kryo.register(DataBusProtocol.Data.class);
        kryo.register(DataBusProtocol.DataList.class);
        kryo.register(DataBusProtocol.PushDataRequest.class);
        kryo.register(DataBusProtocol.PushDataResponse.class);

        kryo.register(WordVectorWithAlpha.class);
        kryo.register(WordVectorWithAlpha[].class);
        kryo.register(HashMapMatrix.class);
        kryo.register(WordVectorUpdate.class);
        kryo.register(WordVectorUpdate[].class);


        kryo.register(SparseWeights.class);
        kryo.register(WeightItem.class);


        return kryo;
    }
}
