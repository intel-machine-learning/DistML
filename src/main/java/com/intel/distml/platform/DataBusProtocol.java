package com.intel.distml.platform;

import com.intel.distml.api.Model;
import com.intel.distml.util.*;

import java.io.*;
import java.nio.channels.SocketChannel;
import java.util.HashMap;

/**
 * DataBusProtocol defines all messages between workers and parameters, and inter workers.
 *
 * Created by taotao on 15-2-3.
 */
public class DataBusProtocol {

    public static final int MSG_FETCH_REQUEST   = 0;
    public static final int MSG_FETCH_RESPONSE  = 1;
    public static final int MSG_PUSH_REQUEST    = 2;
    public static final int MSG_PUSH_RESPONSE   = 3;
    public static final int MSG_SYNC_REQUEST    = 4;
    public static final int MSG_CLOSE           = 5;

    public static abstract class DistMLMessage implements Serializable {

        public final int type;

        public DistMLMessage(int type) {
            this.type = type;
        }

        public int sizeAsBytes(Model model) {
            return 4;
        }

        public void write(AbstractDataWriter out, Model model) throws Exception {
            out.writeInt(type);
        }

        public void read(AbstractDataReader in, Model model) throws Exception {
            // assume type has been read or message construction
        }

        public static DistMLMessage readDistMLMessage(AbstractDataReader in, Model model) throws Exception {
            in.waitUntil(4);
            int type = in.readInt();
            System.out.println("msg type: " + type);
            DistMLMessage msg;
            switch(type) {
                case MSG_FETCH_REQUEST:
                    msg = new FetchRequest();
                    msg.read(in, model);
                    break;
                case MSG_PUSH_REQUEST:
                    msg = new PushRequest();
                    msg.read(in, model);
                    break;
                case MSG_FETCH_RESPONSE:
                    msg = new FetchResponse();
                    msg.read(in, model);
                    break;
                case MSG_PUSH_RESPONSE:
                    msg = new PushResponse();
                    msg.read(in, model);
                    break;
                case MSG_SYNC_REQUEST:
                    msg = new SyncRequest();
                    msg.read(in, model);
                    break;
                default:
                    msg = new CloseRequest();
                    msg.read(in, model);
                    break;
            }

            return msg;
        }
    }

    public static class CloseRequest extends DistMLMessage {

        public CloseRequest() {
            super(MSG_CLOSE);
        }

        @Override
        public int sizeAsBytes(Model model) {
            return super.sizeAsBytes(model);
        }

        @Override
        public void write(AbstractDataWriter out, Model model) throws Exception {
            super.write(out, model);
        }

        @Override
        public void read(AbstractDataReader in, Model model) throws Exception {
            super.read(in, model);
        }
    }

    public static class SyncRequest extends DistMLMessage {

        public SyncRequest() {
            super(MSG_SYNC_REQUEST);
        }

        @Override
        public int sizeAsBytes(Model model) {
            return super.sizeAsBytes(model);
        }

        @Override
        public void write(AbstractDataWriter out, Model model) throws Exception {
            super.write(out, model);
        }

        @Override
        public void read(AbstractDataReader in, Model model) throws Exception {
            super.read(in, model);
        }
    }

    public static abstract class Data extends DistMLMessage {

        public DataDesc format;
        public byte[] data;

        public Data(int type) {
            super(type);
        }

        public Data(int type, DataDesc format, byte[] data) {
            super(type);
            this.format = format;
            this.data = data;
        }

        @Override
        public String toString() {
            return "(Data: len=" + data.length + ")";
        }

        public int sizeAsBytes(Model model) {
            return super.sizeAsBytes(model) + format.sizeAsBytes() + 4 + data.length;
        }

        @Override
        public void write(AbstractDataWriter out, Model model) throws Exception {
            super.write(out, model);
            format.write(out);
            out.writeInt(data.length);
            out.writeBytes(data);
        }

        @Override
        public void read(AbstractDataReader in, Model model) throws Exception {
            super.read(in, model);
            format = new DataDesc();
            in.waitUntil(format.sizeAsBytes() + 4);
            format.read(in);
            int len = in.readInt();
            data = new byte[len];
            in.readFully(data);
        }
    }

    public static class FetchRequest extends DistMLMessage {

        public String matrixName;
        public KeyCollection rows;
        public KeyCollection cols;

        public FetchRequest() {
            super(MSG_FETCH_REQUEST);
        }

        public FetchRequest(String matrixName, KeyCollection rows, KeyCollection cols) {
            super(MSG_FETCH_REQUEST);
            this.matrixName = matrixName;
            this.rows = rows;
            this.cols = cols;
        }

        @Override
        public int sizeAsBytes(Model model) {
            DataDesc format = model.getMatrix(matrixName).getFormat();
            int len1 = matrixName.getBytes().length;
            int len2 = rows.sizeAsBytes(format);
            int len3 = cols.sizeAsBytes(format);
            return  super.sizeAsBytes(model) + 4 + len1 + len2 + len3;
        }

        @Override
        public void write(AbstractDataWriter out, Model model) throws Exception {
            super.write(out, model);
            byte[] d = matrixName.getBytes();
            out.writeInt(d.length);
            out.writeBytes(d);
            DataDesc format = model.getMatrix(matrixName).getFormat();
            rows.write(out, format);
            cols.write(out, format);
        }

        @Override
        public void read(AbstractDataReader in, Model model) throws Exception {
            super.read(in, model);
            in.waitUntil(4);
            int len = in.readInt();
            byte[] buf = new byte[len];
            in.readFully(buf);
            matrixName = new String(buf);
            DataDesc format = model.getMatrix(matrixName).getFormat();
            rows = KeyCollection.readKeyCollection(in, format);
            cols = KeyCollection.readKeyCollection(in, format);
        }
    }


    public static class FetchResponse extends Data {

        public String matrixName;

        public FetchResponse() {
            super(MSG_FETCH_RESPONSE);
        }

        public FetchResponse(String matrixName, DataDesc format, byte[] data) {
            super(MSG_FETCH_RESPONSE, format, data);
            this.matrixName = matrixName;
        }

        @Override
        public String toString() {
            return "(FetchResponse: " + matrixName + ", len=" + data.length + ")";
        }

        @Override
        public int sizeAsBytes(Model model) {
            return super.sizeAsBytes(model) + 4 + matrixName.getBytes().length;
        }

        @Override
        public void write(AbstractDataWriter out, Model model) throws Exception {
            super.write(out, model);
            byte[] d = matrixName.getBytes();
            out.writeInt(d.length);
            out.writeBytes(d);
        }

        @Override
        public void read(AbstractDataReader in, Model model) throws Exception {
            super.read(in, model);
            in.waitUntil(4);
            int len = in.readInt();
            byte[] buf = new byte[len];
            in.readFully(buf);
            matrixName = new String(buf);
        }
    }

    public static class PushRequest extends Data {

        public String matrixName;

        public PushRequest() {
            super(MSG_PUSH_REQUEST);
        }


        public PushRequest(String matrixName, DataDesc format, byte[] data) {
            super(MSG_PUSH_REQUEST, format, data);
            this.matrixName = matrixName;
        }

        @Override
        public String toString() {
            return "(PushRequest: " + matrixName + ", len=" + data.length + ")";
        }

        @Override
        public int sizeAsBytes(Model model) {
            return super.sizeAsBytes(model) + 4 + matrixName.getBytes().length;
        }

        @Override
        public void write(AbstractDataWriter out, Model model) throws Exception {
            super.write(out, model);
            byte[] d = matrixName.getBytes();
            out.writeInt(d.length);
            out.writeBytes(d);
        }

        @Override
        public void read(AbstractDataReader in, Model model) throws Exception {
            super.read(in, model);

            in.waitUntil(4);
            int len = in.readInt();
            byte[] buf = new byte[len];
            in.readFully(buf);
            matrixName = new String(buf);
        }
    }

    public static class PushResponse extends DistMLMessage {
        private static final long serialVersionUID = 1L;

        public boolean success;

        public PushResponse() {
            super(MSG_PUSH_RESPONSE);
        }

        public PushResponse(boolean success) {
            super(MSG_PUSH_RESPONSE);
            this.success = success;
        }

        @Override
        public int sizeAsBytes(Model model) {
            return super.sizeAsBytes(model) + 4;
        }

        @Override
        public void write(AbstractDataWriter out, Model model) throws Exception {
            super.write(out, model);
            out.writeInt(success? 1 : 0);
        }

        @Override
        public void read(AbstractDataReader in, Model model) throws Exception {
            super.read(in, model);
            in.waitUntil(4);
            success = in.readInt() == 1;
        }
    }

}
