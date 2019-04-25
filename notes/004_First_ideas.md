## First primitive sketeches of ideas

1. data parallelism within Word2Vec: Train a dedicated model on each node and average resulting embeddings of all machines to get a single embedding 

2. improve asynchronous SGD by applying further compression techniques (like given for example in [Scalable Distributed DNN Training Using Commodity GPU Cloud Computing](http://nikkostrom.com/publications/interspeech2015/strom_interspeech2015.pdf)
