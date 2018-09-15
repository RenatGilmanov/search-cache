============================
Search Cache 
============================
SearchCache is a specialize cache implementation, designed to properly handle 
search specific data. Generally speaking, there are two critical requirements,
addressed by this implementation:

* to use creation time and access time eviction policies in parallel
* to safely handle large amount of data

This is an experimental cache implementation, based on Javolution FastMap. 
As for now the only intention behind the implementation is to verify several 
cache strategies, related to several usage scenarios. So, it is not intended to be 
used in production.

----------------
Initial tests
----------------
Proper benchmarking is yet to be performed (actually, it will be performed for a bit 
more mature version), but some basic tests, intended to identify complexity behind every single operation, were 
performed.

Both, SearchCache and Guava cache were initialized with the same initial capacity and both expireAfterAccess and
expireAfterWrite eviction strategies::

            searchCache = SearchCache.newBuilder()
                    .initialCapacity(1000)
                    .expireAfterAccess(200) // ms is used by default
                    .expireAfterWrite(600)
                    .build();
                        
            guavaCache = CacheBuilder.newBuilder()
                    .initialCapacity(1000)
                    .expireAfterAccess(200, TimeUnit.MILLISECONDS)
                    .expireAfterWrite(600, TimeUnit.MILLISECONDS)
                    .build();

----------------------
Empty cache benchmark
----------------------
Basically, the idea here is to get a baseline for any further benchmarks.
 
.. image:: doc/images/empty-cache-performance.png?raw=true
   :align: center


--------------------
Pre-populated cache
--------------------
Real-life queries (AOL data) are used for this benchmark.

.. image:: doc/images/preloaded-cache-performance.png?raw=true
   :align: center  

--------------
Conclusion
--------------
Despite the fact, this is just an early results it looks like cache performance can be greatly improved, even 
comparing to Guava cahce implementation.

-------
License
-------
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License a

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
