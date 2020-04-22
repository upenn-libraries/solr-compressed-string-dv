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
package org.apache.solr.schema;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ByteArrayUtf8CharSequence;

/**
 * An extension of {@link StrField} designed to compress DocValues.
 */
public class CompressedStrField extends StrField {

  private static final String DICTIONARY_FILE_ARGNAME = "dictionaryFile";
  private static final String COMPRESSION_LEVEL_ARGNAME = "compressionLevel";
  private static final String COMPRESS_ONLY_WHEN_NECESSARY_ARGNAME = "compressOnlyWhenNecessary";
  private static final int DEFAULT_COMPRESSION_LEVEL = Deflater.BEST_COMPRESSION;
  private static final boolean DEFAULT_COMPRESS_ONLY_WHEN_NECESSARY = false;

  private byte[] dictionary;
  private int compressionLevel;
  private boolean compressOnlyWhenNecessary;
  private ThreadLocal<Deflater> deflater;
  private ThreadLocal<Inflater> inflater;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    String dictionaryFile = args.remove(DICTIONARY_FILE_ARGNAME);
    if (dictionaryFile != null) {
      int outLength = 4096;
      byte[] build = new byte[outLength];
      int size = 0;
      InputStream in = null;
      try {
        in = schema.getResourceLoader().openResource(dictionaryFile);
        int read;
        while ((read = in.read(build, size, outLength - size)) != -1) {
          size += read;
          if (outLength == size) {
            // out of space; resize
            outLength = outLength << 1;
            build = ArrayUtil.growExact(build, outLength);
          }
        }
        dictionary = ArrayUtil.copyOfSubArray(build, 0, size);
      } catch (IOException ex) {
        throw new AssertionError("error reading dictionaryFile: "+dictionaryFile, ex);
      } finally {
        try {
          if (in != null) {
            in.close();
          }
        } catch (IOException ex) {
          throw new AssertionError("error closing dictionaryFile: "+dictionaryFile, ex);
        }
      }
    }
    String tmp = args.remove(COMPRESSION_LEVEL_ARGNAME);
    this.compressionLevel = tmp == null ? DEFAULT_COMPRESSION_LEVEL : Integer.parseInt(tmp);
    tmp = args.remove(COMPRESS_ONLY_WHEN_NECESSARY_ARGNAME);
    this.compressOnlyWhenNecessary = tmp == null ? DEFAULT_COMPRESS_ONLY_WHEN_NECESSARY : Boolean.parseBoolean(tmp);
    initFlaters();
    //super.init(schema, args);
  }

  private void initFlaters() {
    deflater = new ThreadLocal<Deflater>() {
      @Override
      protected Deflater initialValue() {
        Deflater ret = new Deflater(compressionLevel, true);
        return ret;
      }

      @Override
      public Deflater get() {
        Deflater ret = super.get();
        ret.reset();
        if (dictionary != null) {
          ret.setDictionary(dictionary);
        }
        return ret;
      }
    };
    inflater = new ThreadLocal<Inflater>() {
      @Override
      protected Inflater initialValue() {
        Inflater ret = new Inflater(true);
        return ret;
      }
      @Override
      public Inflater get() {
        Inflater ret = super.get();
        ret.reset();
        if (dictionary != null) {
          ret.setDictionary(dictionary);
        }
        return ret;
      }
    };
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value) {
    if (field.indexed() || field.stored() || !field.hasDocValues()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "field type "+CompressedStrField.class.getName()+
          " must have docValues, and must be neither indexed nor stored");
    } else {
      IndexableField docval;
      final BytesRef bytes = getCompressed(value);
      if (field.multiValued()) {
        docval = new SortedSetDocValuesField(field.getName(), bytes);
      } else {
        docval = new SortedDocValuesField(field.getName(), bytes);
      }
      return Collections.singletonList(docval);
    }
  }

  private BytesRef getCompressed(Object value) {
    if (value instanceof ByteArrayUtf8CharSequence) {
      ByteArrayUtf8CharSequence utf8 = (ByteArrayUtf8CharSequence) value;
      return deflate(utf8.getBuf(), utf8.offset(), utf8.size());
    } else {
      final String origStrValue = value.toString();
      final byte[] utf8 = new byte[UnicodeUtil.maxUTF8Length(origStrValue.length())];
      final int length = UnicodeUtil.UTF16toUTF8(origStrValue, 0, origStrValue.length(), utf8);
      return deflate(utf8, 0, length);
    }
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return inflate(term).utf8ToString();
  }

  @Override
  public CharsRef indexedToReadable(BytesRef input, CharsRefBuilder output) {
    output.copyUTF8Bytes(inflate(input));
    return output.get();
  }

  private BytesRef inflate(BytesRef input) {
    final int expectedSize = readVInt(input);
    if (expectedSize == 0) {
      // not compressed
      return input;
    }
    byte[] res = new byte[expectedSize];
    final Inflater i = this.inflater.get();
    i.setInput(input.bytes, input.offset, input.length);
    final int actualSize;
    try {
      actualSize = i.inflate(res);
    } catch (DataFormatException ex) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, ex);
    }
    assert actualSize == expectedSize;
    return new BytesRef(res, 0, actualSize);
  }

  private static final int MAX_DOCVALUES_BYTES = 32766; //TODO: where is this from? Point to some other static var? DocumentsWriterPerThread.MAX_TERM_LENGTH_UTF8?

  private BytesRef deflate(byte[] buf, int offset, final int originalSize) {
    if (originalSize == 0 || (compressOnlyWhenNecessary && originalSize < MAX_DOCVALUES_BYTES - 1)) {
      return uncompressed(buf, offset, originalSize);
    }
    final Deflater d = this.deflater.get();
    d.setInput(buf, offset, originalSize);
    byte[] out = new byte[originalSize + VINT_MAX_BYTES]; // safe if we can assume that deflated stream will be smaller (?)
    final int startCompressed = writeVInt(originalSize, out);
    d.finish();
    int compressedSize = d.deflate(out, startCompressed, originalSize, Deflater.FULL_FLUSH);
    if (d.finished()) {
      return new BytesRef(out, 0, compressedSize);
    } else {
      return uncompressed(buf, offset, originalSize);
    }
  }

  private BytesRef uncompressed(byte[] buf, int offset, final int originalSize) {
    final int sizeWithHeader = originalSize + 1;
    byte[] out = new byte[sizeWithHeader]; // first byte will be 0 (also vint 0)
    System.arraycopy(buf, offset, out, 1, originalSize);
    return new BytesRef(out, 0, sizeWithHeader);
  }

  private static final int VINT_MAX_BYTES = 5;

  /**
   * Special method for variable length int (copied from lucene). Usually used for writing the length of a
   * collection/array/map In most of the cases the length can be represented in one byte (length &lt; 127) so it saves 3
   * bytes/object
   */
  public static int writeVInt(int i, byte[] bs) {
    int iOut = 0;
    while ((i & ~0x7F) != 0) {
      bs[iOut++] = ((byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    bs[iOut++] = ((byte) i);
    return iOut;
  }

  public static int readVInt(BytesRef input) {
    byte[] bs = input.bytes;
    int offset = input.offset;
    byte b = bs[offset++];
    int i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = bs[offset++];
      i |= (b & 0x7F) << shift;
    }
    input.length -= offset - input.offset;
    input.offset = offset;
    return i;
  }

}
