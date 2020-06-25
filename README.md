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

## Pipeline

1. S3 Bucket With Existing Files
2. Run pull-bucket-v2
   * Gets list of files in S3 bucket from step 1
   * Chunks each file into an IPLD Block which is stored in s3
   * Saves information about each file and generated IPLD Block CIDs in dynamo
3. Run create-parts-v2
   * Reads list of files and IPLD Block CIDs from dynamo created in step 2
   * Generates Car files from the IPLD blocks and writes them to S3

## Environment Variables

* DUMBO_COMMP_TABLE - dynamo table name for commp?
* DUMBO_CREATE_PART_LAMBDA - lambda function name to create parts from files
* DUMBO_PARSE_FILE_LAMBDA - labmda function name to parse file 

## Getting Started

This project includes support Visual Studio Code Remote Containers which 
enables everything needed for development and running in a docker container.
You can use this by installing:

* Visual Studio Code
  * Remote Containers Extension
* Docker Desktop (for Mac or Windows) or Docker CE (for Linux)

Once these are installed, open the folder of this project in Visual Studio
Code and left click the green box in the bottom right and choose "Reopen in
container".  The first time you do this, a docker container will be built
with all pre-requisites - this may take a few minutes.  Once the building is
done, you can open a shell in the container via: Terminal->New Terminal or
(Control+Shift+`) 

