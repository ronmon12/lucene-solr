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
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import static org.apache.lucene.index.StoredFieldVisitor.Status.STOP;

/**
 * Created by rlmathes on 7/15/17.
 */
public class EmbeddedDBStoredFieldsReader extends StoredFieldsReader{

    /** Extension of stored fields file */
    public static final String FIELDS_EXTENSION = "fdt";
    private Directory directory;
    private SegmentInfo si;
    private FieldInfos infos;
    private IOContext context;
    private String readHandle;
    private IndexInput fieldsStream;
    private boolean closed;

    /** Used only by clone. */
    private EmbeddedDBStoredFieldsReader(FieldInfos fieldInfos, IndexInput fieldsStream, String readHandle) {
        this.infos = fieldInfos;
        this.fieldsStream = fieldsStream;
        this.readHandle = readHandle;
    }

    /** Main constructor */
    public EmbeddedDBStoredFieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {

        this.directory = directory;
        this.si = si;
        this.infos = fn;
        this.context = context;
        boolean success = false;

        try {
            fieldsStream = directory.openInput(IndexFileNames.segmentFileName(si.name, "", FIELDS_EXTENSION), context);
            final String writerUUID = fieldsStream.readString();
            fieldsStream.close();
            final StringBuilder handleBuilder = new StringBuilder(writerUUID);
            handleBuilder.append("_");
            handleBuilder.append(si.name);
            readHandle = handleBuilder.toString();
            success = true;
        } finally {
            // With lock-less commits, it's entirely possible (and
            // fine) to hit a FileNotFound exception above. In
            // this case, we want to explicitly close any subset
            // of things that were opened so that we don't have to
            // wait for a GC to do so.
            if (!success) {
                try {
                    close();
                } catch (Throwable t) {} // ensure we throw our original exception
            }
        }

    }

    @Override
    public void visitDocument(int n, StoredFieldVisitor visitor) throws IOException {

        StringBuilder documentKeyBuilder = new StringBuilder(readHandle);
        documentKeyBuilder.append("_");
        documentKeyBuilder.append(n);
        EDBDocument document = BerkeleyDBStore.INSTANCE.get(documentKeyBuilder.toString());

        for(EDBStoredField field : document.getFields()) {

            FieldInfo info = this.infos.fieldInfo(field.getName());
            if(null == info) {
                continue;
            }

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
        ensureOpen();
        return new EmbeddedDBStoredFieldsReader(this.infos, this.fieldsStream, this.readHandle);
    }

    /**
     * @throws AlreadyClosedException if this FieldsReader is closed
     */
    private void ensureOpen() throws AlreadyClosedException {
        if (closed) {
            throw new AlreadyClosedException("this FieldsReader is closed");
        }
    }

    @Override
    public void checkIntegrity() throws IOException {

    }

    /**
     * Closes the underlying {@link org.apache.lucene.store.IndexInput} streams.
     * This means that the Fields values will not be accessible.
     *
     * @throws IOException If an I/O error occurs
     */
    @Override
    public final void close() throws IOException {
        if (!closed) {
            IOUtils.close(fieldsStream);
            closed = true;
        }
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

}
