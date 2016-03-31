/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.Sentence;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author david
 */
public class StanfordParser {
    public List<List<TaggedWord>> posTagString(String inputString) throws IOException {
        List<List<TaggedWord>> tSentences;
        tSentences = new ArrayList<>();
        MaxentTagger tagger = new MaxentTagger("database/english-left3words-distsim.tagger");
        List<List<HasWord>> sentences = MaxentTagger.tokenizeText(new StringReader(inputString));
        for (List<HasWord> sentence : sentences) {
            List<TaggedWord> tSentence = tagger.tagSentence(sentence);
            tSentences.add(tSentence);
        }
        return tSentences;
    }
    
        public void postTag() throws FileNotFoundException {

        MaxentTagger tagger = new MaxentTagger("database/english-left3words-distsim.tagger");
        List<List<HasWord>> sentences = MaxentTagger.tokenizeText(new BufferedReader(new FileReader("./database/dataset.txt")));
        for (List<HasWord> sentence : sentences) {
            List<TaggedWord> tSentence = tagger.tagSentence(sentence);
            System.out.println(Sentence.listToString(tSentence, false));
        }
    }
}
