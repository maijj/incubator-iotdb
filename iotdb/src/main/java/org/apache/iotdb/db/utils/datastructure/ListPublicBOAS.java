package org.apache.iotdb.db.utils.datastructure;

import java.io.OutputStream;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

/**
 * We reimplement {@linkplain PublicBAOS}, replacing the underlying {@linkplain
 * java.io.ByteArrayOutputStream} with a {@code List } of {@code byte[]}.
 *
 * For efficient and controllable GC, all {@code byte[]} are allocated from {@linkplain
 * ListPublicBAOSPool} and should be put back after {@code close}.
 *
 * Referring to {@linkplain TVList} and {@linkplain org.apache.iotdb.db.rescon.PrimitiveArrayPool PrimitiveArrayPool}.
 *
 * So far, this class is only used in {@linkplain org.apache.iotdb.db.engine.memtable.MemTableFlushTask
 * MemTableFlushTask}.
 *
 * @author Pengze Lv, kangrong
 */
public class ListPublicBOAS extends PublicBAOS {

  public ListPublicBOAS() {
    super();
    // TODO
    throw new UnsupportedOperationException();
  }

  public ListPublicBOAS(int size) {
    super(size);
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void write(byte b[], int off, int len) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void write(int b) {
    // TODO
    throw new UnsupportedOperationException();
  }


  public byte[] getBuf() {
    // TODO
    throw new UnsupportedOperationException();
  }

  public synchronized void reset() {
    // TODO
    throw new UnsupportedOperationException();
  }

  public synchronized int size() {
    // TODO
    throw new UnsupportedOperationException();

  }

  public void close() {
    // TODO
    throw new UnsupportedOperationException();
  }

  /**
   * We are not sure whether following functions will be invoked in PublicBOAS. We'd defer to
   * implement them, but throw exception to avoid unexpected calling.
   */
  public synchronized void writeTo(OutputStream out) {
    throw new UnsupportedOperationException();
  }


  public synchronized byte toByteArray()[] {
    throw new UnsupportedOperationException();
  }

  public synchronized String toString() {
    throw new UnsupportedOperationException();
  }

  public synchronized String toString(String charsetName) {
    throw new UnsupportedOperationException();
  }


}

