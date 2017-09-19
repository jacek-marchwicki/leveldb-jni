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

import java.io.Closeable;

public class LevelIterator implements Closeable {
    private final long nativePointer;
    private boolean closed = false;

    @Keep
    LevelIterator(long nativePointer) {
        this.nativePointer = nativePointer;
    }

    @Override
    public void close() {
        if (!closed) {
            nativeClose(nativePointer);
            closed = true;
        }
    }

    public void seekToFirst(byte[] startingKey) throws LevelDBException {
        checkIfNotClosed();
        nativeSeekToFirst(nativePointer, startingKey);
    }

    private void checkIfNotClosed() throws LevelDBException {
        if (closed) {
            throw new LevelDBException("Iterator closed");
        }
    }

    public void seekToFirst() throws LevelDBException {
        checkIfNotClosed();
        nativeSeekToFirst(nativePointer);
    }


    public boolean isValid() throws LevelDBException {
        checkIfNotClosed();
        return nativeIsValid(nativePointer);
    }

    public byte[] key() throws LevelDBException {
        checkIfNotClosed();
        return nativeKey(nativePointer);
    }


    public byte[] value() throws LevelDBException {
        checkIfNotClosed();
        return nativeValue(nativePointer);
    }

    public void next() throws LevelDBException {
        checkIfNotClosed();
        nativeNext(nativePointer);
    }

    @Keep
    private native void nativeClose(long nativePointer);
    @Keep
    private native void nativeSeekToFirst(long nativePointer);
    @Keep
    private native void nativeSeekToFirst(long nativePointer, byte[] startingKey);
    @Keep
    private native boolean nativeIsValid(long nativePointer);
    @Keep
    private native byte[] nativeKey(long nativePointer);
    @Keep
    private native byte[] nativeValue(long nativePointer);
    @Keep
    private native void nativeNext(long nativePointer);

}
