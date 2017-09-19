/*
 * Copyright [2016] <jacek.marchwicki@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.appunite.leveldb;

import android.support.annotation.Keep;
import android.text.TextUtils;

import java.io.Closeable;

public class LevelDB implements Closeable {

    static {
        System.loadLibrary("leveldb-jni");
    }

    private final long nativeDB;
    private boolean closed = false;

    public LevelDB(String path) throws LevelDBException {
        checkPath(path);
        nativeDB = nativeOpen(path);
    }

    @Override
    public void close() {
        if (!closed) {
            nativeClose(nativeDB);
            closed = true;
        }
    }

    public void putBytes(byte[] key, byte[] value) throws LevelDBException {
        checkIfNotClosed();
        checkKey(key);
        checkValue(value);
        nativePutBytes(nativeDB, key, value);
    }

    private void checkIfNotClosed() throws LevelDBException {
        if (closed) {
            throw new LevelDBException("Database closed");
        }
    }

    public byte[] getBytes(byte[] key) throws LevelDBException, KeyNotFoundException {
        checkIfNotClosed();
        checkKey(key);
        return nativeGetBytes(nativeDB, key);
    }

    public void delete(byte[] key) throws LevelDBException {
        checkIfNotClosed();
        checkKey(key);
        nativeDelete(nativeDB, key);
    }

    public boolean exists(byte[] key) throws LevelDBException {
        checkIfNotClosed();
        checkKey(key);
        return nativeExists(nativeDB, key);
    }

    public LevelIterator newInterator() throws LevelDBException {
        checkIfNotClosed();
        return nativeIterator(nativeDB);
    }

    public void write(WriteBatch batch) throws LevelDBException {
        checkIfNotClosed();
        if (batch == null) {
            throw new NullPointerException("Batch can not be null");
        }
        nativeWrite(nativeDB, batch.nativePointer);
    }


    public static void destroy(String path) throws LevelDBException {
        checkPath(path);
        nativeDestroy(path);
    }

    private static void checkPath(String path) {
        if (TextUtils.isEmpty(path)) {
            throw new NullPointerException("path can not be null");
        }
    }

    static void checkValue(Object value) {
        if (value == null) {
            throw new NullPointerException("value parameter can not be null");
        }
    }

    static void checkKey(byte[] key) {
        if (key == null || key.length == 0) {
            throw new NullPointerException("key parameter can not be null");
        }
    }

    @Keep
    private native long nativeOpen(String databasePath);
    @Keep
    private native void nativeClose(long nativePointer);
    @Keep
    private native void nativePutBytes(long nativePointer, byte[] key, byte[] value);
    @Keep
    private native byte[] nativeGetBytes(long nativePointer, byte[] key);
    @Keep
    private native void nativeDelete(long nativePointer, byte[] key);
    @Keep
    private native boolean nativeExists(long nativePointer, byte[] key);
    @Keep
    private native LevelIterator nativeIterator(long nativePointer);
    @Keep
    private static native void nativeDestroy(String databasePath);
    @Keep
    private native void nativeWrite(long nativePointer, long writeBatchNativePointer);

}
