Squishy: Scala DSL / Bindings for SQS (Amazon Web Services)
===========================================================

Squishy is a Scala library for interacting with the [Amazon Simple Queue Service](http://aws.amazon.com/sqs/) (SQS)
messaging API. The goals of this project are to provide a concise interface for developers to interact with SQS and at
the same time make SQS-related code more modular and easier to test.

There are two main components to the Squishy library:

 1. The Scala-SQS interface is a small wrapper around the SQS client library for Java. This wrapper allows developers to
    interact with SQS using Scala types and idioms, which makes the resulting code easier to develop and maintain.
 2. Also included is an in-memory implementation of the SQS API that is intended to be used for testing purposes. This
    part of the library is just the SQS API wrapped around a couple of off-the-shelf data structures included in the
    Java runtime. There is no production-level code in this mock SQS implementation: it does not support sending
    messages over the network, persisting messages to disk, or anything else that would be expected of a real messaging
    service.
    
API Documentation is [available online](http://lpryor.github.io/squishy/#squishy.package).

See [CHANGELOG] for the project history.

### Building ###

You need SBT 0.12.x or higher.

    # Compile and package .jars.
    sbt package

    # Generate API documentation.
    sbt doc

### Examples ###

See [src/test/scala/squishy/examples] for examples of Squishy in action.

### Target platform ###

* Scala 2.9.0+
* JVM 1.5+

### License ###

Squishy is is licensed under the terms of the
[Apache Software License v2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

