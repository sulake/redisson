/**
 * Copyright (c) 2013-2020 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.iterator;

import org.redisson.AsyncIterator;
import org.redisson.ScanResult;
import org.redisson.api.RFuture;
import org.redisson.client.RedisClient;
import org.redisson.misc.RedissonPromise;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionStage;

abstract class RedissonBaseMapAsyncIterator<T> implements AsyncIterator<T> {

    private RedisClient client;
    private volatile RFuture<ScanResult<T>> batch;

    private CompletionStage<Iterator<T>> scan() {
        if (this.batch == null) {
            this.batch = iterator(client, 0);
        }
        return batch.thenCompose(scanResult -> {
            final Iterator<T> iterator = scanResult.getValues().iterator();
            if (!iterator.hasNext()) {
                batch = iterator(client, scanResult.getPos());
                client = scanResult.getRedisClient();
                return batch.thenApply(r -> r.getValues().iterator());
            }
            return RedissonPromise.newSucceededFuture(iterator);
        });
    }

    @Override
    public RFuture<Boolean> hasNext() {
        final RedissonPromise<Boolean> result = new RedissonPromise<>();
        scan().thenAccept(iterator -> {
            result.trySuccess(iterator != null && iterator.hasNext());
        });
        return result;

    }

    @Override
    public RFuture<T> next() {
        final RedissonPromise<T> result = new RedissonPromise<>();
        scan().thenAccept(iterator -> {
            if (iterator == null) {
                result.tryFailure(new NoSuchElementException());
            } else {
                result.trySuccess(iterator.next());
            }
        });
        return result;
    }

    protected abstract RFuture<ScanResult<T>> iterator(RedisClient client, long nextIteratorPos);
}
