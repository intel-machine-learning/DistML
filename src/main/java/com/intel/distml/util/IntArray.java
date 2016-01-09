package com.intel.distml.util;

import akka.pattern.Patterns;
import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Session;
import com.intel.distml.platform.DataBusProtocol;
import com.intel.distml.util.KeyCollection;
import scala.concurrent.Future;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by yunlong on 12/8/15.
 */
public class IntArray extends SparseArray<Long, Integer> {

    public IntArray(long dim) {
        super(dim, DataDesc.KEY_TYPE_LONG, DataDesc.ELEMENT_TYPE_INT);
    }

}
