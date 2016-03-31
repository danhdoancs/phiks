/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author david
 */
public class StopWords {

    public List<TaggedWord> removeStopWords(List<List<TaggedWord>> queryTermset) {
        List<TaggedWord> termset = new ArrayList<>();
        int k = 0;
        String sCurrentLine;

        List<String> stopwords = new ArrayList();
        String termPattern = "";
        try {
            FileReader fr = new FileReader("database/stopwordslist.txt");
            //C:\Users\son\Documents\advdbmsfinal\database
            BufferedReader br = new BufferedReader(fr);
            while ((sCurrentLine = br.readLine()) != null) {
                stopwords.add(sCurrentLine);
            }

            for (Iterator<List<TaggedWord>> sentenceIt = queryTermset.iterator(); sentenceIt.hasNext();) {
                List<TaggedWord> sentence = sentenceIt.next();

                for (int ii = 0; ii < sentence.size(); ii++) {
                    String term = sentence.get(ii).value();
                    
                    //check symbol contain only character
                    if (!term.matches("^[a-z]{1,10}$")) {
                        continue;
                    }

                    //check stopword
                    if (!stopwords.contains(term)) {
                        termset.add(sentence.get(ii));
                    }
                }
            }
        } catch (Exception ex) {
            System.out.println(ex);
        }

        return termset;
    }

    public void removeDuplication(List<TaggedWord> queryTermset) {
        int size = queryTermset.size();

        for (int i = 0; i < size - 1; i++) {
            TaggedWord iWord = queryTermset.get(i);
            for (int j = i + 1; j < size; j++) {
                if (iWord.equals(queryTermset.get(j))) {
                    queryTermset.remove(j--);
                    size--;
                }
            }
        }
    }
}
