mdinc
=======================

> Tooling for incremental verification of Merkle-Damgård construction hash functions (e.g. SHA2/3)

Merkle-Damgård based hash functions like SHA2 cannot be incrementally verified.
This means that given `SHA2-256(10GB data)` when trying to retrieve the corresponding data
all 10GB of it must be downloaded in order to confirm that it matches.
This is quite problematic in peer-to-peer environments where individual peers are untrusted and
needing to download large amounts of data before verification is an attack vector (e.g. common IPFS deployments).

This package relies on leveraging freestart-collision resistance within secure Merkle-Damgård hash functions
like SHA2-256 to allow for incremental verification of hashes of large amounts of data.
For example, allowing for verifying 1MiB of data at a time of a 10GB object.

## Documentation

This repo is a work-in-progress and still early stage. Look at `--help` on the CLI
or the documentation on exported public functions for more information.

## References

- Talk on block limits and the strategy employed here ([video](https://youtu.be/pCvnB6tW6_g), [slides](https://docs.google.com/presentation/d/1WLCMCxzQDaITi93x-wIfChp2O0yMy-24VgkyJ0hhrgY))
- A related discuss.ipfs.tech [thread](https://discuss.ipfs.tech/t/supporting-large-ipld-blocks) discussing the issues with large blocks in IPFS
- A [demo](https://bafybeifhfiynbz36lbho2pow6h3w5hxj2orimr6zcjjxkzdj2vw5ekk62y.ipfs.w3s.link/docker-ipfs-demo.mkv) leveraging this repo to demonstrate pulling Docker images over IPFS without relying on a special Docker registry (i.e. DockerHub is fine)

## Contributing

Contributions are welcome!
This work is largely related to the IPFS Project and you can find people interested in it at the IPFS community forums and chat channels https://docs.ipfs.tech/community/.

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
