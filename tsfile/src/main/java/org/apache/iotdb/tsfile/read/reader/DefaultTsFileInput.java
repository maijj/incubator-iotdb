/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.read.reader;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class DefaultTsFileInput implements TsFileInput {

  FileChannel channel;
  MappedByteBuffer mappedByteBuffer;
  boolean enableMMap = true;

  public DefaultTsFileInput(Path file) throws IOException {
    channel = FileChannel.open(file, StandardOpenOption.READ);
    if (enableMMap && channel.size() < 2147483647L) {
      mappedByteBuffer = channel.map(MapMode.READ_ONLY, 0, channel.size());
      mappedByteBuffer.slice();
    }
  }

  @Override
  public long size() throws IOException {
    return channel.size();
  }

  @Override
  public long position() throws IOException {
    if (mappedByteBuffer != null) {
      return mappedByteBuffer.position();
    }
    return channel.position();
  }

  @Override
  public TsFileInput position(long newPosition) throws IOException {
    if (mappedByteBuffer != null) {
      mappedByteBuffer.position((int) newPosition);
    }
    channel.position(newPosition);
    return this;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    if (mappedByteBuffer != null) {
      mappedByteBuffer.get(dst.array());

    }
    return channel.read(dst);
  }

  @Override
  public int read(ByteBuffer dst, long position) throws IOException {

    if (mappedByteBuffer != null) {
      int oldPosition = dst.position();
      for (int i = dst.position(); i < dst.capacity(); i++) {
        dst.put(i, mappedByteBuffer.get((int) (i + position)));
      }
      dst.position(dst.capacity());
      return dst.capacity() - oldPosition;
    }

    return channel.read(dst, position);
  }

  @Override
  public int read() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileChannel wrapAsFileChannel() throws IOException {
    return channel;
  }

  @Override
  public InputStream wrapAsInputStream() throws IOException {
    return Channels.newInputStream(channel);
  }

  @Override
  public void close() throws IOException {
    channel.close();
    if (mappedByteBuffer != null) {
      clean(mappedByteBuffer);
    }
  }

  @Override
  public int readInt() throws IOException {
    throw new UnsupportedOperationException();
  }


  public static void clean(MappedByteBuffer mappedByteBuffer) {
    ByteBuffer buffer = mappedByteBuffer;
    if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0) {
      return;
    }
    invoke(invoke(viewed(buffer), "cleaner"), "clean");
  }

  private static Object invoke(final Object target, final String methodName,
      final Class<?>... args) {
    return AccessController.doPrivileged(new PrivilegedAction<Object>() {
      public Object run() {
        try {
          Method method = method(target, methodName, args);
          method.setAccessible(true);
          return method.invoke(target);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
    });
  }

  private static Method method(Object target, String methodName, Class<?>[] args)
      throws NoSuchMethodException {
    try {
      return target.getClass().getMethod(methodName, args);
    } catch (NoSuchMethodException e) {
      return target.getClass().getDeclaredMethod(methodName, args);
    }
  }

  private static ByteBuffer viewed(ByteBuffer buffer) {
    String methodName = "viewedBuffer";
    Method[] methods = buffer.getClass().getMethods();
    for (int i = 0; i < methods.length; i++) {
      if (methods[i].getName().equals("attachment")) {
        methodName = "attachment";
        break;
      }
    }
    ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
    if (viewedBuffer == null) {
      return buffer;
    } else {
      return viewed(viewedBuffer);
    }
  }

}
