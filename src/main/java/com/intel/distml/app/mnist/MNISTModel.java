package com.intel.distml.app.mnist;

import com.intel.distml.model.cnn.*;
import com.intel.distml.util.Matrix;

import java.util.List;

/**
 * Created by yunlong on 2/8/15.
 */
public class MNISTModel extends ConvModel {

    public static final int INPUT_IMAGE_WIDTH = 28;
    public static final int INPUT_IMAGE_HEIGHT = 28;
    public static final float eta=1.0f;//fixed learning rate
    public static float[] rL;
    public static int itrIndex;
    public MNISTModel() {

        super(6);
        itrIndex=0;
        rL=new float[600];//TODO:fix the magic number

        ImageLayer input = new MNISTInputLayer(this, 28, 28, 1);
        addLayer(0, input);

        ConvLayer c1 = new ConvLayer(this, "c1", input, 5, 5, 6);
        addLayer(1, c1);

        PoolLayer s1 = new PoolLayer(this, "s1", c1, 2);
        addLayer(2, s1);

        ConvLayer c2 = new ConvLayer(this, "c2", s1, 5, 5, 12);
        addLayer(3, c2);

        PoolLayer s2 = new PoolLayer(this, "s3", c2, 2);
        addLayer(4, s2);

        FullConnectionLayer full = new FullConnectionLayer(this, "full", s2, 10);
        addLayer(5, full);  // partitioned.
    }

    @Override
    public Matrix transformSamples(List<Object> samples) {
        String str = (String)samples.get(0);
        System.out.println("sample string: [" + str + "]");
        String[] strs = str.split(",");

        LabeledImage data = new LabeledImage(INPUT_IMAGE_WIDTH, INPUT_IMAGE_HEIGHT);
        float[][] img = data.element(0);
        for (int i = 0; i < INPUT_IMAGE_WIDTH; i++) {
            for (int j = 0; j < INPUT_IMAGE_HEIGHT; j++) {
                img[i][j] = Integer.parseInt(strs[i*INPUT_IMAGE_WIDTH+j]) * 1.0f / 255;
            }
        }

        data.label = Integer.parseInt(strs[INPUT_IMAGE_WIDTH*INPUT_IMAGE_HEIGHT]);
        //data.show();
        return data;
    }


}
