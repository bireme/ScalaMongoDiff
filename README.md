# ScalaMongoDiff

Compare two MongoDB collections, examining each document and field in detail, to create a third collection that records the differences.

### App versions
* Scala 2.13
* JDK 19
* MongoDB 5.0.13

### Execution parameters

1. Navigate to the 'bin' directory and run the 'mongocolldiff.sh' script, providing the required parameters and, if necessary, the optional ones, example:

        ...\bin>./mongocolldiff.sh -database_from1=dbname1 -collection_from1=colname1 -collection_from2=colname2 -collection_out=colnameout -idField=idname

   Parameters:

         -database_from1=<name>           - Name of the MongoDB database
         -collection_from1=<name>         - Name of the collection in the database
         -collection_from2=<name>         - Name of the collection in the database
         -collection_out=<name>           - Name of the collection in the database
         -idField=<name>                  - Document identifier field
         [-database_from2=<name>]         - MongoDB database name, by default, will be the database_from1
         [-database_out=<name>]           - MongoDB database name, by default, will be the database_from1
         [-host_from1=<name>]             - MongoDB server name. Default is 'localhost'
         [-port_from1=<number>]           - MongoDB server port number. Default is 27017
         [-host_from2=<name>]             - MongoDB server name. Default is 'localhost'
         [-port_from2=<number>]           - MongoDB server port number. Default is 27017
         [-host_out=<name>]               - MongoDB server name. Default is 'localhost'
         [-port_out=<number>]             - MongoDB server port number. Default is 27017
         [-user_from1=<name>]             - MongoDB user name
         [-password_from1=<password>]     - MongoDB user password
         [-user_from2=<name>]             - MongoDB user name
         [-password_from2=<password>]     - MongoDB user password
         [-user_out=<name>]               - MongoDB user name
         [-password_out=<password>]       - MongoDB user password
         [-total=<number>]                - Total documents to be compared
         [-noCompFields=<name>]           - Fields that should not be compared
         [-takeFields=<name>]             - Fields that should be included in the new collection, even if compared
         [--noUpDate]                     - If present, it will not add the _updd field with the update date
         [--append]                       - If present, it will compose the collection without clearing it first