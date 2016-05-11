/**
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

import android.text.TextUtils;

import java.io.Closeable;

public class LevelDB implements Closeable {

    static {
        System.loadLibrary("leveldb-jni");
    }

    @SuppressWarnings("unused")
    /**
     * We need use this field inside JNI
     */
    private long nativeDB;

    public LevelDB(String path) throws LevelDBException {
        checkPath(path);
        nativeOpen(path);
    }

    @Override
    public void close() {
        nativeClose();
    }

    public void putBytes(byte[] key, byte[] value) throws LevelDBException {
        checkKey(key);
        checkValue(value);
        nativePutBytes(key, value);
    }

    public byte[] getBytes(byte[] key) throws LevelDBException, KeyNotFoundException {
        checkKey(key);
        return nativeGetBytes(key);
    }

    public void delete(byte[] key) throws LevelDBException {
        checkKey(key);
        nativeDelete(key);
    }

    public boolean exists(byte[] key) throws LevelDBException {
        checkKey(key);
        return nativeExists(key);
    }

    public LevelIterator newInterator() throws LevelDBException {
        return nativeIterator();
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

    private static void checkValue(Object value) {
        if (value == null) {
            throw new NullPointerException("value parameter can not be null");
        }
    }

    private static void checkKey(byte[] key) {
        if (key == null || key.length == 0) {
            throw new NullPointerException("key parameter can not be null");
        }
    }

    private native void nativeOpen(String databasePath);
    private native void nativeClose();
    private native void nativePutBytes(byte[] key, byte[] value);
    private native byte[] nativeGetBytes(byte[] key);
    private native void nativeDelete(byte[] key);
    private native boolean nativeExists(byte[] key);
    private native LevelIterator nativeIterator();
    private static native void nativeDestroy(String databasePath);

}
