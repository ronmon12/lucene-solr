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

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

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

        for(EDBStoredField field : document.getFields()) {

            FieldInfo info = this.fn.fieldInfo(field.name);
            if(null != field.stringValue) {
                visitor.stringField(info, field.stringValue);
            }
            else if(null != field.numericValue) {
                Number number = field.numericValue;
                if (number instanceof Integer) {
                    visitor.intField(info, (Integer) field.numericValue);
                } else if (number instanceof Long) {
                    visitor.longField(info, (Long) field.numericValue);
                } else if (number instanceof Float) {
                    visitor.floatField(info, (Float) field.numericValue);
                } else if (number instanceof Double) {
                    visitor.doubleField(info, (Double) field.numericValue);
                } else {
                    throw new IllegalArgumentException("cannot store numeric type " + number.getClass());
                }
            }
            else if(null != field.binaryValue) {
                visitor.binaryField(info, field.binaryValue);
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
}
