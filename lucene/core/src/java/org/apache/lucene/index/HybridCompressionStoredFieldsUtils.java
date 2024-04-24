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

package org.apache.lucene.index;

import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Hybrid Compression Utils
 */
public final class HybridCompressionStoredFieldsUtils {

    /**
     * Hybrid Compression Ctor
     */
    public HybridCompressionStoredFieldsUtils(){

    }

    /** No Compression Mode from here. */
    public static final CompressionMode NO_COMPRESSION =
            new CompressionMode() {
                @Override
                public Compressor newCompressor() {
                    return new Compressor() {
                        @Override
                        public void close() throws IOException {
                        }

                        @Override
                        public void compress(ByteBuffersDataInput buffersInput, DataOutput out)
                                throws IOException {
                            out.copyBytes(buffersInput, buffersInput.size());
                        }
                    };
                }

                @Override
                public Decompressor newDecompressor() {
                    return new Decompressor() {
                        @Override
                        public void decompress(
                                DataInput in, int originalLength, int offset, int length, BytesRef bytes)
                                throws IOException {
                            bytes.bytes = ArrayUtil.grow(bytes.bytes, length);
                            in.skipBytes(offset);
                            in.readBytes(bytes.bytes, 0, length);
                            bytes.offset = 0;
                            bytes.length = length;
                        }

                        @Override
                        public Decompressor clone() {
                            return this;
                        }
                    };
                }
            };
}