/*
 * Copyright [2017] <jacek.marchwicki@gmail.com>
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

public class WriteBatch {

    static {
        System.loadLibrary("leveldb-jni");
    }

    final long nativePointer;

    public WriteBatch() {
        this.nativePointer = nativeCreate();
    }


    public void putBytes(byte[] key, byte[] value) throws LevelDBException {
        LevelDB.checkKey(key);
        LevelDB.checkValue(value);
        nativePutBytes(nativePointer, key, value);
    }

    public void delete(byte[] key) throws LevelDBException {
        LevelDB.checkKey(key);
        nativeDelete(nativePointer, key);
    }

    public void clear() {
        nativeClear(nativePointer);
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        nativeFree(nativePointer);
    }

    @Keep
    private native long nativeCreate();
    @Keep
    private native void nativeDelete(long nativePointer, byte[] key);
    @Keep
    private native void nativePutBytes(long nativePointer, byte[] key, byte[] value);
    @Keep
    private native void nativeFree(long nativePointer);
    @Keep
    private native void nativeClear(long nativePointer);


}
