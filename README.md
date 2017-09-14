# Zero-copy Paxos

## How to build

Quick-start:

    mkdir -p bin
    g++ -O3 -Wall -Werror -pthread -o bin/node   -DNDEBUG -DNTRACE -Isrc/h -std=c++11 $(find node src/c  -type f)
    g++ -O3 -Wall -Werror -pthread -o bin/client -DNDEBUG -DNTRACE -Isrc/h -std=c++11 $(find client      -type f)
    g++ -O3 -Wall -Werror -pthread -o bin/test                     -Isrc/h -std=c++11 $(find tests src/c -type f)
    bin/test

This does a simple build of the `release` versions of the node and client
binaries and a build and run of the test suite.

For development work, you probably want incremental builds. The build system
uses [Shake](http://shakebuild.com/). If you haven't got an existing Haskell
environment then install
[stack](https://docs.haskellstack.org/en/stable/README/#how-to-install) and
then run:

    cd build
    stack build --exec build

This creates binaries in
`build/_build/{release,debug,trace}/{node,client,test}`. After making changes,
running

    stack exec -- build

rebuilds everything as needed.
