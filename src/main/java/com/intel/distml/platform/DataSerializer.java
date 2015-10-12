package com.intel.distml.platform;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Model;
import com.intel.distml.api.Partition;
import com.intel.distml.api.PartitionInfo;
import com.intel.distml.model.cnn.*;
import com.intel.distml.model.lda.*;
import com.intel.distml.model.rosenblatt.PointSample;
import com.intel.distml.model.rosenblatt.Rosenblatt;
import com.intel.distml.model.rosenblatt.Weights;
import com.intel.distml.model.sparselr.SparseWeights;
import com.intel.distml.model.sparselr.WeightItem;
import com.intel.distml.model.word2vec.*;
import com.intel.distml.util.*;
import com.intel.distml.util.primitive.IntArray;
import com.intel.distml.util.primitive.IntMatrix;
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

        kryo.register(KeyCollection.ALL_KEYS.class);
        kryo.register(KeyCollection.EMPTY_KEYS.class);
        kryo.register(KeyList.class);
        kryo.register(KeyHash.class);
        kryo.register(KeyRange.class);
        kryo.register(GeneralArray.class);
        kryo.register(IntArray.class);
        kryo.register(IntMatrix.class);

        kryo.register(DataBusProtocol.FetchModelRequest.class);
        kryo.register(DataBusProtocol.FetchModelResponse.class);
        kryo.register(DataBusProtocol.PartialDataRequest.class);
        kryo.register(DataBusProtocol.Data.class);
        kryo.register(DataBusProtocol.DataList.class);
        kryo.register(DataBusProtocol.PushDataRequest.class);
        kryo.register(DataBusProtocol.PushDataResponse.class);

        kryo.register(Model.class);
        kryo.register(Partition.class);
        kryo.register(PartitionInfo.class);
        kryo.register(PartitionInfo.Type.class);
        kryo.register(DMatrix.class);

        kryo.register(Word2VecModel.class);
        kryo.register(WordTree.class);
        kryo.register(WordNode.class);
        kryo.register(WordNode[].class);
        kryo.register(UpdateMatrix.class);
        kryo.register(ParamMatrix.class);
        kryo.register(WordVectorWithAlpha.class);
        kryo.register(WordVectorWithAlpha[].class);
        kryo.register(HashMapMatrix.class);
        kryo.register(WordVectorUpdate.class);
        kryo.register(WordVectorUpdate[].class);

        kryo.register(LDAModel.class);
        kryo.register(Dictionary.class);
        kryo.register(ParamTopic.class);
        kryo.register(ParamWordTopic.class);
        kryo.register(Topic.class);
        kryo.register(WordTopic.class);

        kryo.register(Rosenblatt.class);
        kryo.register(Rosenblatt.SensorNodes.class);
        kryo.register(PointSample.class);
        kryo.register(Weights.class);

        kryo.register(SparseWeights.class);
        kryo.register(WeightItem.class);

        kryo.register(float[][].class);
        kryo.register(float[][][].class);
        kryo.register(float[][][][].class);
        kryo.register(Float[][].class);
        kryo.register(ConvKernels.class);
        kryo.register(ConvEdge.class);
        kryo.register(ConvLayer.class);
        kryo.register(ImageLayer.class);
        kryo.register(ImagesData.class);
        kryo.register(LabeledImage.class);
        kryo.register(PoolLayer.class);
        kryo.register(PoolEdge.class);
        kryo.register(PoolLayerParam.class);
        kryo.register(ConvModel.class);
        kryo.register(FullConnectionLayer.class);
        kryo.register(FullConnectionMatrix.class);
        kryo.register(FullConnectionVector.class);
        kryo.register(FullConnectionEdge.class);

        return kryo;
    }
}
