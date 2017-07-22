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

/**
 * Created by rlmathes on 7/15/17.
 */
public class EmbeddedDBStoredFieldsWriter extends StoredFieldsWriter {

    private SegmentKey segmentKey;
    private SegmentData segmentData;
    private EDBStoredDocument currentEDBStoredDocument;
    private int currentDocumentKey = 0;

    public EmbeddedDBStoredFieldsWriter(Directory directory, String segment, IOContext context) {

        segmentKey = new SegmentKey(segment);
        segmentData = new SegmentData();
        EmbeddedDBStore.INSTANCE.put(segmentKey, segmentData);
    }

    @Override
    public void startDocument() throws IOException {
        currentEDBStoredDocument = new EDBStoredDocument();
    }

    @Override
    public void finishDocument() throws IOException {

        segmentData.putDocument(currentDocumentKey, currentEDBStoredDocument);
        EmbeddedDBStore.INSTANCE.put(segmentKey, segmentData);
        currentDocumentKey++;
    }

    @Override
    public void writeField(FieldInfo info, IndexableField field) throws IOException {

        EDBStoredField EDBStoredField = new EDBStoredField();
        EDBStoredField.name = field.name();

        if(null != field.stringValue()) {
            EDBStoredField.stringValue = field.stringValue();
        }
        else if(null != field.numericValue()) {
            EDBStoredField.numericValue = field.numericValue();
        }
        else if(null != field.binaryValue()) {
            EDBStoredField.binaryValue = field.binaryValue().bytes;
        }
        currentEDBStoredDocument.addField(EDBStoredField);
    }

    @Override
    public void abort() {

    }

    @Override
    public void finish(FieldInfos fis, int numDocs) throws IOException {

    }

    @Override
    public void close() throws IOException {

        segmentKey = null;
        currentEDBStoredDocument = null;
    }
}
