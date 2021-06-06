/*
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.perf.toolstest;

import java.util.concurrent.locks.LockSupport;
import org.apache.storm.utils.MutableLong;
import org.jctools.queues.MpscArrayQueue;

class Cons extends MyThd {
    public final MutableLong counter = new MutableLong(0);
    private final MpscArrayQueue<Object> queue;

    public Cons(MpscArrayQueue<Object> queue) {
        super("Consumer");
        this.queue = queue;
    }

    @Override
    public void run() {
        Handler handler = new Handler();
        long start = System.currentTimeMillis();

        while (!halt) {
            int x = queue.drain(handler);
            if (x == 0) {
                LockSupport.parkNanos(1);
            } else {
                counter.increment();
            }
        }
        runTime = System.currentTimeMillis() - start;
    }

    @Override
    public long getCount() {
        return counter.get();
    }

    private class Handler implements org.jctools.queues.MessagePassingQueue.Consumer<Object> {
        @Override
        public void accept(Object event) {
            counter.increment();
        }
    }
}
