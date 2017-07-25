package org.apache.lucene;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.embeddeddb.EmbeddedDBCodec;
import org.apache.lucene.codecs.embeddeddb.EmbeddedDBStore;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;
import org.junit.Assert;
import org.junit.Test;


/**
 *
 * Simple test for experimenting indexing with different codecs and directories
 *
 */
public class TestCodecOperations extends LuceneTestCase {

  @Test
  public void testWritesAndReads() throws IOException {

    EmbeddedDBStore.INSTANCE.reinitialize();

    List<Document> inputDocuments = new ArrayList<>();
    Document docOne = new Document();
    docOne.add(newTextField("vehicles", "car, truck, van", Field.Store.YES));
    Document docTwo = new Document();
    docTwo.add(newTextField("vehicles", "car, car, van", Field.Store.YES));
    Document docThree = new Document();
    docThree.add(newTextField("vehicles", "car, van, van", Field.Store.YES));
    Field field = new LongField("vehiclesCount", 4L, Field.Store.YES);
    Document docFour = new Document();
    docFour.add(field);

    inputDocuments.add(docOne);
    inputDocuments.add(docTwo);
    inputDocuments.add(docThree);
    inputDocuments.add(docFour);
    
    Analyzer analyzer = new MockAnalyzer(random());
    Directory directory = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, analyzer);
    config.setCodec(new EmbeddedDBCodec());
    IndexWriter indexWriter = new IndexWriter(directory, config);
    indexWriter.addDocuments(inputDocuments);
    indexWriter.close();

    IndexReader indexReader = DirectoryReader.open(directory);
    List<Document> readDocuments = new ArrayList<>();
    for(int i = 0; i < indexReader.numDocs(); i++) {
      readDocuments.add(indexReader.document(i));
    }
    indexReader.close();

    Assert.assertTrue(readDocuments.size() == 4);
    for(int i = 0; i < 3; i++) {
      String inputvehicles = inputDocuments.get(i).getField("vehicles").stringValue();
      String readvehicles = readDocuments.get(i).getField("vehicles").stringValue();
      assertEquals(inputvehicles, readvehicles);
    }

    Assert.assertEquals(field.numericValue(), readDocuments.get(3).getField("vehiclesCount").numericValue());
    directory.close();
    EmbeddedDBStore.INSTANCE.purge();
    EmbeddedDBStore.INSTANCE.close();
  }

  @Test
  public void testMerge() throws IOException {

    EmbeddedDBStore.INSTANCE.reinitialize();

    List<Document> inputDocs1 = new ArrayList<>();
    Document docOne = new Document();
    docOne.add(newTextField("vehicles", "car, truck, van", Field.Store.YES));
    Document docTwo = new Document();
    docTwo.add(newTextField("vehicles", "car, car, van", Field.Store.YES));
    inputDocs1.add(docOne);
    inputDocs1.add(docTwo);
    List<Document> inputDocs2 = new ArrayList<>();
    Document docThree = new Document();
    docThree.add(newTextField("vehicles", "truck, truck, van", Field.Store.YES));
    Document docFour = new Document();
    docFour.add(newTextField("vehicles", "boat, boat, boat", Field.Store.YES));
    inputDocs2.add(docThree);
    inputDocs2.add(docFour);

    Analyzer analyzer = new MockAnalyzer(random());
    Directory directory = newDirectory();

    IndexWriterConfig config1 = new IndexWriterConfig(Version.LATEST, analyzer);
    config1.setCodec(new EmbeddedDBCodec());
    IndexWriter writer1 = new IndexWriter(directory, config1);
    writer1.addDocuments(inputDocs1);
    writer1.close();
    DirectoryReader reader = DirectoryReader.open(directory);
    SegmentReader segmentReader1 = getOnlySegmentReader(reader);
    assertEquals("_0", segmentReader1.getSegmentName());
    segmentReader1.close();

    IndexWriterConfig config2 = new IndexWriterConfig(Version.LATEST, analyzer);
    config2.setCodec(new EmbeddedDBCodec());
    IndexWriter writer2 = new IndexWriter(directory, config2);
    writer2.addDocuments(inputDocs2);

    writer2.forceMerge(1);
    writer2.close();
    reader = DirectoryReader.open(directory);
    SegmentReader segmentReader2 = getOnlySegmentReader(reader);
    assertEquals("_2", segmentReader2.getSegmentName());
    segmentReader2.close();

    directory.close();
    EmbeddedDBStore.INSTANCE.purge();
    EmbeddedDBStore.INSTANCE.close();
  }

  @Test
  public void testDemo() throws IOException {

    EmbeddedDBStore.INSTANCE.reinitialize();

    Analyzer analyzer = new MockAnalyzer(random());
    Directory directory = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig(Version.LATEST, analyzer);
    config.setCodec(new EmbeddedDBCodec());

    //Write the index
    IndexWriter iwriter = new IndexWriter(directory, config);
    Document doc = new Document();
    String longTerm = "longtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongtermlongterm";
    String text = "This is the text to be indexed. " + longTerm;
    doc.add(newTextField("fieldname", text, Field.Store.YES));
    iwriter.addDocument(doc);
    iwriter.close();
    
    //Search the index
    IndexReader ireader = DirectoryReader.open(directory); // read-only=true
    IndexSearcher isearcher = newSearcher(ireader);
    assertEquals(1, isearcher.search(new TermQuery(new Term("fieldname", longTerm)), 1).totalHits);
    Query query = new TermQuery(new Term("fieldname", "text"));
    TopDocs hits = isearcher.search(query, null, 1);
    assertEquals(1, hits.totalHits);

    // Iterate through the results:
    for (int i = 0; i < hits.scoreDocs.length; i++) {
      Document hitDoc = isearcher.doc(hits.scoreDocs[i].doc);
      assertEquals(text, hitDoc.get("fieldname"));
    }

    // Test simple phrase query
    PhraseQuery phraseQuery = new PhraseQuery();
    phraseQuery.add(new Term("fieldname", "to"));
    phraseQuery.add(new Term("fieldname", "be"));
    assertEquals(1, isearcher.search(phraseQuery, null, 1).totalHits);

    ireader.close();
    directory.close();
    EmbeddedDBStore.INSTANCE.purge();
    EmbeddedDBStore.INSTANCE.close();
  }
}
