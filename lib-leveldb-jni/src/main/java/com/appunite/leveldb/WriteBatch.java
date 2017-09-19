package com.appunite.leveldb;

public class WriteBatch {
    @SuppressWarnings("unused")
    /**
     * We need use this field inside JNI
     */
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

    private native long nativeCreate();
    private native void nativeDelete(long nativePointer, byte[] key);
    private native void nativePutBytes(long nativePointer, byte[] key, byte[] value);
    private native void nativeFree(long nativePointer);
    private native void nativeClear(long nativePointer);


}
