/*
 * Copyright 2009-2010 MBTE Sweden AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
@Typed package primenumbers

import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.CountDownLatch

abstract class Sieve {
    abstract Collection<Integer> factor(int value)
}

class BlockingSieve extends Sieve {
    final ArrayList<Pair<Integer,Integer>> primes = [[2,4],[3,9]]
    int maxChecked = 3
    private final ReentrantReadWriteLock lock = []

    Collection<Integer> factor(int value) {
        lock.readLock().lock()
        try {
            return factor(value, 0, [])
        }
        finally {
            lock.readLock().unlock()
        }
    }

    private boolean isFactorOfKnownPrimes(int value) {
        for(p in primes) {
            if(value < p.second)
                return false
            
            if(!(value % p.first))
                return true
        }
        false
    }

    private Collection<Integer> factor(int value, int startFrom, Collection<Integer> collected) {
        if(value == 1)
            return collected

        for(int i = startFrom;  i < primes.size(); ) {
            def p = primes[i]
            if(p.second > value) {
                return collected << value
            }

            if(!(value % p.first))
                return factor(value / p.first, i, collected << p.first)

            if(i == primes.size()-1) {
                if(maxChecked > 46341) {
                    return collected << value
                }

                def mc = maxChecked + 2
                def check = isFactorOfKnownPrimes(mc)

                lock.readLock().unlock()
                lock.writeLock().lock()

                try {
                    if(mc == maxChecked+2) {
                        if(!check) {
                                primes.add([mc,mc*mc])
                            i++
                        }

                        maxChecked = mc
                    }
                }
                finally {
                    lock.readLock().lock()
                    lock.writeLock().unlock()
                }
            }
            else {
                i++
            }
        }
    }
}

class NonBlockingSieve extends Sieve {
    final int [] primes = new int [4792*2]
    volatile long state

    private static final long MC_MASK = (-1L <<  32)
    private static final long SZ_MASK = (-1L >>> 32)
    private static final long INC_MC = 0x0000000200000000L
    private static final long INC_MC_SZ = 0x0000000200000002L

    NonBlockingSieve() {
        primes[0]  = 2
        primes[1]  = 4
        primes[2]  = 3
        primes[3]  = 9

        state = (3L << 32) + 4
    }

    Collection<Integer> factor(int value) {
        return factor(value, 0, [])
    }

    private boolean isFactorOfKnownPrimes(int value) {
        int sz = state & SZ_MASK
        for(def i = 0; i < sz; i += 2) {
            int p
            while(!(p = primes[i+1])) {
                sz = state & SZ_MASK
            }

            if(value < primes[i+1])
                return false

            if(!(value % primes[i]))
                return true
        }
        false
    }

    private Collection<Integer> factor(int value, int startFrom, Collection<Integer> collected) {
        if(value == 1)
            return collected

        int i = startFrom
        while(true) {
            def s = state
            int p
            while(!(p = primes[i+1])) {
                s = state
            }

            if(p > value) {
                return collected << value
            }

            p = primes[i]
            if(!(value % p))
                return factor(value / p, i, collected << p)

            int sz = s & SZ_MASK
            if(i == sz-2) {
                int maxChecked = (s & MC_MASK) >> 32
                if(maxChecked > 46341) {
                    return collected << value
                }

                def mc = maxChecked + 2
                def check = isFactorOfKnownPrimes(mc)

                if(!check) {
                    if(state.compareAndSet(s, s + INC_MC_SZ) ) {
                        i += 2
                        primes [i]   = mc
                        primes [i+1] = mc*mc
                    }
                }
                else {
                    state.compareAndSet(s, s + INC_MC)
                }
            }
            else {
                i += 2
            }
        }
    }
}

println "started"

def pool = Executors.newFixedThreadPool(256)

for(threads in [1,2,4,8,16,32,64,128,256,128,64,32,16,8,4,2,1]) {
    for(sieve in [new NonBlockingSieve (), new BlockingSieve ()]) {
        def numbersPerThreads = 2000000 / threads
        CountDownLatch cdl = [threads]
        println "$threads started ${sieve.class.simpleName}"
        def start = System.currentTimeMillis()
        for(i in 0..<threads) {
            pool {
                def baseNum = Integer.MAX_VALUE - threads * numbersPerThreads + i * numbersPerThreads
                for(j in 0..<numbersPerThreads) {
                    def number = baseNum + j
                    def factors = sieve.factor(number)
//                    def r = 1
//                    for(f in factors) {
//                        assert isPrimeNaive(f)
//                        r *= f
//                    }
//                    assert r == number
//
//                    println "T$i: $increment $number -> $factors"
                }
                cdl.countDown()
            }
        }
        cdl.await()
        println "done in ${System.currentTimeMillis()-start} ms\n"
    }
}
pool.shutdown()
