
Quick Intro

A keyed object gets its own csv file via Object Relational Mapping, and I try not to touch these except through tools.  This is what I call the mockdb.  All the information for all objects of that class are stored there.  For each class, I have an object that is essentially a dictionary of the following form {key:object}.  This allows me to keep the information in memory keyed to the primary object key.

Some objects, like Case and Control, are "children" of a higher category object (Sample), and I keep track of children through class inheritance (I also can figure out the ancestors and progenitor).  All children can be loaded for a given object, if requested.  This is how we could write RapidRunSequencing and HighThroughputSequencing objects, and either load them individually or pull them together in the parent object SequencingRun, which could also have a separate csv file for data where the run type is unknown.

Like relational databases, the key for another object is often stored within the data for a given object.  For instance, a barcode-object has information specifying both the sample to which it was attached and the lane in which it was run.  This approach also makes data extraction very intuitive. 

Processes, such a zcat or bcbio-gen, are also keyed objects and stored in a similar fashion.  Thus to run the pipeline, I just have to load in the data to the correct objects, link the objects, and tell the process object to run.
