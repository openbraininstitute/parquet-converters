NeuronParquet
=============

Neuron parquet is a collection of tools to convert functionalizer input and output data to parquet.

Building
--------
In order to build it, several dependencies must be met:
  1. Arrow (libarrow)
  2. Parquet-cpp (libparquet)
  3. Thrift
  4. Snappy

The suggested way is to compile them from the source to enable for static linking.
In that case we STRONGLY recommend building and installing Arrow first with basic dependencies, using the cmake options:
  $ cmake .. -DCMAKE_BUILD_TYPE=release -DCMAKE_PREFIX_PATH=/usr/local -DARROW_WITH_BROTLI=OFF -DARROW_WITH_ZSTD=OFF -DARROW_USE_SSE=ON -DARROW_BUILD_TESTS=OFF -DARROW_WITH_LZ4=OFF

  When building (running make) cmake will download and build arrow dependencies (including Snappy).
  The next steps are therefore:
  $ make install
  $ cd snappy_ep/src/snappy_ep-install
  $ cp * /usr/local/ -R

  A similar procedure happens with Parquet-cpp. It shall find the version of Arrow we built/installed, and will download/build Thrift for us.
  The steps are, roughly:
  $ cmake .. -DCMAKE_PREFIX_PATH=/usr/local -DCMAKE_BUILD_TYPE=release
  $ make
  $ make install
  $ cd thrift_ep/src/thrift_ep-install/
  $ cp * /usr/local/ -R

When dependencies are ready, compile Neuron Parquet:
$ cd build && cmake .. -DCMAKE_PREFIX_PATH=/usr/local -DCMAKE_BUILD_TYPE=release
$ make install

