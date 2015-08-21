package com.intel.distml.model.lda;

import com.intel.distml.api.DMatrix;
import com.intel.distml.api.Model;
import com.intel.distml.api.ModelWriter;
import com.intel.distml.api.databus.ServerDataBus;
import com.intel.distml.util.Matrix;
import com.sun.org.apache.xpath.internal.operations.Mod;
import scala.util.parsing.combinator.testing.Str;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.IdentityHashMap;

/**
 * Created by ruixiang on 7/24/15.
 */
public class LDAModelWriter implements ModelWriter {

    static class Pair{
        public int wordID;
        public int count;
        Pair(int _wordID, int _count){
            wordID = _wordID;
            count = _count;
        }
    }

    @Override
    public void writeModel(Model model, ServerDataBus dataBus) {
        for (String matrixName: model.dataMap.keySet()) {
            DMatrix m = model.dataMap.get(matrixName);
            if (!m.hasFlag(DMatrix.FLAG_PARAM))
                continue;

            Matrix result = dataBus.fetchFromServer(matrixName);
            //m.setLocalCache(result);
            writeToFile(matrixName,result,matrixName + ".txt",model);
        }
    }

    public void writeToFile(String matrixName, Matrix m, String filePath,Model model){

        if(matrixName == LDAModel.MATRIX_PARAM_TOPIC){
            Topic t = (Topic)m;
            Integer[] values = t.values;

            StringBuilder tmp = new StringBuilder();
            for(int i = 0;i < values.length;i++) {
                tmp.append(values[i]);
                tmp.append(',');
            }
            tmp.deleteCharAt(tmp.length() - 1);

            BufferedWriter bw=null;
            try {
                bw = new BufferedWriter(new FileWriter(new File(filePath)));
                bw.write(tmp.toString().toCharArray());
                bw.close();
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }

        if(matrixName == LDAModel.MATRIX_PARAM_WORDTOPIC){
            WordTopic wt = (WordTopic) m;
            Integer[][] values = wt.values;
            BufferedWriter bw=null;

            //write the word-topic matrix
            try {
                bw = new BufferedWriter(new FileWriter(new File(filePath)));

                for(int i=0;i<values.length;i++) {

                    StringBuilder tmp = new StringBuilder();
                    tmp.append(((LDAModel)model).dict.getWord(i));
                    tmp.append(":");
                    for(int j = 0;j<values[i].length;j++) {
                        tmp.append(values[i][j]);
                        tmp.append(',');
                    }
                    tmp.deleteCharAt(tmp.length()-1);

                    bw.write(tmp.toString().toCharArray());
                    bw.newLine();
                }

                bw.close();

            }
            catch (IOException e){
                e.printStackTrace();
            }

            //write the topic rank
            try {
                bw = new BufferedWriter(new FileWriter(new File("rank-" + filePath)));


                int Num=((LDAModel)model).dict.getSize() > 5?5:((LDAModel)model).dict.getSize();
                Pair[] rankedWords = new Pair[Num];
                for(int i = 0;i < Num;i++){
                    rankedWords[i]=new Pair(0,0);
                }

                for(int i=0;i<values[0].length;i++) {
                    bw.write("topic: " +  i);
                    bw.newLine();

                    for(int j=0;j<Num;j++) {
                        int max=-1;int pos=0;
                        for (int k = 0; k < values.length; k++) {
                            if(values[k][i]>max){
                                max=values[k][i];
                                pos=k;
                            }
                            rankedWords[j].wordID=pos;
                            rankedWords[j].count=max;
                            values[pos][i]=0;//remove  the max number
                            bw.write(((LDAModel) model).dict.getWord(pos) + ":" +max);
                            bw.newLine();
                        }
                    }
                    bw.newLine();
                    bw.newLine();
                }

                bw.close();

            }
            catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
