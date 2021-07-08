# ArBlockStore

[Arweave](https://www.arweave.org/) is a project that allows permanent
storage of data for a one-time fee.  This is particularly interesting
for archival usecases.
ArBlockStore is a project and tool that allows storing blocks of Bitcoin-like
blockchains into Arweave, to preserve them for eternity even if the
projects themselves should disappear in the future.

This is a simple Python utility, which queries the blockchain daemon
via JSON-RPC for the blocks with increasing height, and submits Arweave
transactions with them.

## Structure of the Generated Arweave Data

The Arweave transactions contain the raw block data as payload, and have the
following tags for indexing and searching on Arweave directly:

    App-Name: ArBlockStore
    App-Version: 1.0
    Blockchain: Namecoin
    Block-Height: 100
    Block-Hash: 000000000049ab1ce075326ba9c92e16aebb255813ee4cab6e24ad5eb2506527
    Previous-Hash: 00000000006279083d1c92f1fd4cb29d6ca87a9a20387edd14db91421c26082a

This data alone (without looking at the payload) is enough to build up
the full tree of blocks from the genesis onward, and allows quick retrieval
of individual blocks by hash / height or of all blocks for one particular
blockchain.

Note that anyone is able to create transactions with fake blocks and wrong
data, so blocks should either be validated or filtered by the creator
wallet as needed!

## Running

The tool is very easy to run on the command-line.  It needs the following
dependencies installed:

    pip install arweave-python-client jsonrpclib-pelix

Then it can be executed as follows to query for and archive blocks in a
range of heights:

    ./arblockstore.py write \
        --blockchain Namecoin \
        --rpc "http://user:password@localhost:8336/" \
        --wallet "/path/to/arweave/wallet/file" \
        --from 0 \
        --to 100
