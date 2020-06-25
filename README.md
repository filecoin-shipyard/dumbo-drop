# Massive Parallel Graph Builder

This project uses Lambda to create massive IPLD graphs
and then break them down into many .car files for storage
in Filecoin.

This project also uses Dynamo tables (currently hard coded)
to store intermediary information about the graph as it is
built.

## Setup

* Create block bucket
* Create Dynamo Table named `dumbo-v2-{source-bucket-name}`
  * partition key must be "url"
  * configure bucket to have its capacity "on-demand"
* run `arc deploy`
  * get lambda function names


## Staging

The graphs we're building are so large that we need to do them in stages.

TODO

