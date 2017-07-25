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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import static org.apache.lucene.index.StoredFieldVisitor.Status.STOP;

/**
 * Created by rlmathes on 7/15/17.
 */
public class EmbeddedDBStoredFieldsReader extends StoredFieldsReader{

    private Directory directory;
    private SegmentInfo si;
    private FieldInfos fn;
    private IOContext ioContext;
    private SegmentData segmentData;

    public EmbeddedDBStoredFieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) {

        this.directory = directory;
        this.si = si;
        this.fn = fn;
        this.ioContext = context;

        SegmentKey segmentKey = new SegmentKey(si.name);
        segmentData = EmbeddedDBStore.INSTANCE.get(segmentKey);
    }

    @Override
    public void visitDocument(int n, StoredFieldVisitor visitor) throws IOException {

        EDBStoredDocument document = segmentData.getDocument(n);
        if(null == document) {
            Logger.LOG(LogLevel.WARN, "No document stored for ID = " + n);
            return;
        }

        for(EDBStoredField field : document.getFields()) {

            FieldInfo info = this.fn.fieldInfo(field.getName());

            if(visitor.needsField(info) == StoredFieldVisitor.Status.YES) {
                if(null != field.getStringValue()) {
                    visitor.stringField(info, field.getStringValue());
                }
                else if(null != field.getNumericValue()) {
                    Number number = field.getNumericValue();
                    if (number instanceof Integer) {
                        visitor.intField(info, (Integer) field.getNumericValue());
                    } else if (number instanceof Long) {
                        visitor.longField(info, (Long) field.getNumericValue());
                    } else if (number instanceof Float) {
                        visitor.floatField(info, (Float) field.getNumericValue());
                    } else if (number instanceof Double) {
                        visitor.doubleField(info, (Double) field.getNumericValue());
                    } else {
                        throw new IllegalArgumentException("cannot store numeric type " + number.getClass());
                    }
                }
                else if(null != field.getBinaryValue()) {
                    int endOffset = field.getOffset() + field.getLength();
                    visitor.binaryField(info, Arrays.copyOfRange(field.getBinaryValue(), field.getOffset(), endOffset));
                }
            }
            else if(visitor.needsField(info) == STOP) {
                return;
            }

        }

    }

    @Override
    public StoredFieldsReader clone() {
        EmbeddedDBStoredFieldsReader reader = new EmbeddedDBStoredFieldsReader(this.directory, this.si, this.fn, this.ioContext);
        reader.setSegmentData(this.segmentData);
        return reader;
    }

    @Override
    public void checkIntegrity() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    public void setSegmentData(SegmentData segmentData) {
        this.segmentData = segmentData;
    }

    public Map<Integer, EDBStoredDocument> getDocumentsForMerge() {
        final Map<Integer, EDBStoredDocument> documentsForMerge = new HashMap<>();
        documentsForMerge.putAll(segmentData.getAllDocuments());
        if(documentsForMerge.size() < 1) {
            Logger.LOG(LogLevel.INFO, "Segment " + si.name + " is returning no documents for merge");
        }
        return segmentData.getAllDocuments();
    }

}
