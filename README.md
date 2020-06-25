# Massive Parallel Graph Builder

This project uses Lambda to create massive IPLD graphs
and then break them down into many .car files for storage
in Filecoin.

This project also uses Dynamo tables (currently hard coded)
to store intermediary information about the graph as it is
built.

## Staging

The graphs we're building are so large that we need to do them in stages.

TODO

