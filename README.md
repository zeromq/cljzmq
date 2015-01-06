# cljzmq

[![Build Status](https://travis-ci.org/zeromq/cljzmq.png)](https://travis-ci.org/zeromq/cljzmq)

Clojure bindings for ØMQ.

## Installation

```clj
[org.zeromq/cljzmq "0.1.4"]
```
(see [FAQ](https://github.com/zeromq/cljzmq/wiki/FAQ) for how to make it work with jeromq)

### Snapshots

To use the latest and greatest snapshot, add the OSS Sonatype repository to your `project.clj` file:

```clj
["sonatype" {:url "https://oss.sonatype.org/content/repositories/snapshots"
             :update :always}]
```

Add the following dependency to your `project.clj` file:

```clj
[org.zeromq/cljzmq "0.1.5-SNAPSHOT"]
```

## Documentation

* [Changelog](https://github.com/zeromq/cljzmq/blob/master/CHANGES.md)
* [API docs](http://zeromq.github.io/cljzmq)
* [FAQ](https://github.com/zeromq/cljzmq/wiki/FAQ)
* [Examples](https://github.com/trevorbernard/cljzmq-examples)

## Acknowledgements

YourKit is kindly supporting ZeroMQ project with its full-featured [Java Profiler](http://www.yourkit.com/java/profiler/index.jsp).

## License

Copyright © 2013-2015 Contributors as noted in the AUTHORS.md file

This is free software; you can redistribute it and/or modify it under the terms
of the GNU Lesser General Public License as published by the Free Software
Foundation; either version 3 of the License, or (at your option) any later
version.
