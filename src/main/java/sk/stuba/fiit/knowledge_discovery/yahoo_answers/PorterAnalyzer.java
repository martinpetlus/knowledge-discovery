package sk.stuba.fiit.knowledge_discovery.yahoo_answers;

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.TypeTokenFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

// http://stackoverflow.com/questions/25714455/standardanalyzer-with-stemming
public class PorterAnalyzer extends AnalyzerWrapper {

    private Analyzer baseAnalyzer;

    public PorterAnalyzer() {
        this.baseAnalyzer = new StandardAnalyzer(Version.LUCENE_46);
    }

    @Override
    public void close() {
        baseAnalyzer.close();
        super.close();
    }

    @Override
    protected Analyzer getWrappedAnalyzer(final String fieldName) {
        return baseAnalyzer;
    }

    @Override
    protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
        TokenStream ts = components.getTokenStream();

        Set<String> filteredTypes = new HashSet<String>();
        filteredTypes.add("<NUM>");

        TypeTokenFilter numberFilter = new TypeTokenFilter(Version.LUCENE_46, ts, filteredTypes);

        PorterStemFilter porterStem = new PorterStemFilter(numberFilter);

        return new TokenStreamComponents(components.getTokenizer(), porterStem);
    }

}
