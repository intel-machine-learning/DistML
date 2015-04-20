package com.intel.distml.model.cnn;

import com.intel.distml.util.KeyRange;

/**
 * Created by yunlong on 2/16/15.
 */
public class LabeledImage extends ImagesData {

    public int label;

    public LabeledImage(int imageWidth, int imageHeight) {

        super(imageWidth, imageHeight, new KeyRange(0, 0));

        label = 0;
    }
}
