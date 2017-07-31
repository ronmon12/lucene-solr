package org.apache.lucene.codecs.embeddeddb;

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
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;

/**
 * Created by rlmathes on 7/15/17.
 */
public class EmbeddedDBStoredFieldsWriter extends StoredFieldsWriter {

    private String segmentName;
    private EDBDocument currentDocument;

    public EmbeddedDBStoredFieldsWriter(Directory directory, String segment, IOContext context) {
        segmentName = segment;
    }

    @Override
    public void startDocument() throws IOException {
        currentDocument = new EDBDocument();
    }

    @Override
    public void finishDocument() throws IOException {
        BerkeleyDBStore.INSTANCE.put(segmentName, currentDocument);
    }

    @Override
    public void writeField(FieldInfo info, IndexableField field) throws IOException {

        EDBStoredField edbStoredField = new EDBStoredField();
        edbStoredField.setName(field.name());

        if(null != field.numericValue()) {
            edbStoredField.setNumericValue(field.numericValue());
        }
        else if(null != field.binaryValue()) {
            edbStoredField.setBinaryValue(field.binaryValue().bytes);
            edbStoredField.setOffset(field.binaryValue().offset);
            edbStoredField.setLength(field.binaryValue().length);
        }
        else if(null != field.stringValue()) {
            edbStoredField.setStringValue(field.stringValue());
        }

        currentDocument.addField(edbStoredField);
    }

    private static int nextLiveDoc(int doc, Bits liveDocs, int maxDoc) {
        if (liveDocs == null) {
            return doc;
        }
        while (doc < maxDoc && !liveDocs.get(doc)) {
            ++doc;
        }
        return doc;
    }

    @Override
    public void abort() {

    }

    @Override
    public void finish(FieldInfos fis, int numDocs) throws IOException {
    }

    @Override
    public void close() {

    }
}
