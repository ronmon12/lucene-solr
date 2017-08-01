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
    private FieldInfos infos;
    private IOContext ioContext;
    private String segmentName;

    public EmbeddedDBStoredFieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) {
        this.directory = directory;
        this.si = si;
        this.infos = fn;
        this.ioContext = context;
        segmentName = si.name;
    }

    @Override
    public void visitDocument(int n, StoredFieldVisitor visitor) throws IOException {

        EDBDocument document = BerkeleyDBStore.INSTANCE.get(segmentName, n);
        if(this.infos.size() == 0 || null == document) {
            return;
        }

        for(EDBStoredField field : document.getFields()) {

            FieldInfo info = this.infos.fieldInfo(field.getName());

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
                else {
                    Logger.LOG(LogLevel.WARN, "Field retrieval was attempted, but value was not recognized.");
                }
            }
            else if(visitor.needsField(info) == STOP) {
                return;
            }
        }
    }

    @Override
    public StoredFieldsReader clone() {
        EmbeddedDBStoredFieldsReader reader = new EmbeddedDBStoredFieldsReader(this.directory, this.si, this.infos, this.ioContext);
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

}
