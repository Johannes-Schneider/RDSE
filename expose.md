## Expose

### Motivation

* Bleeding edge of NLP
* Baseline implementations take a comparably long time for training
* Generally, available text corpi tend to continuously grow over time
* Research work is often hindered by too long training times
* Model accurracy improves as the training data grows, processing huge amounts of input data is therefore desirable
* Interesting challenge to deal with such huge amounts of data

### Related Work

* Existing implementations often use multi CPU and even multi GPU acceleration for decreasing training time [4]
* Recently, models outgrew main memory of commodity servers during training [1][2][3] --> Distributing model training becomes neccesarry
* Approaches for distributed training often use at least one of the following concepts:
  * Data parallel (training data is partitioned, model is replicated)
  * Model parallel (model parameters are partitioned, data is replicated)
* First implementations made use of a centralized parameter server for synchronizing various workers, however those implementations yield the risk of a single point of failure [5] --> Asynchronous implementations to avoid this issue [3]

### Approach

* First implement basic synchronous distributed word embedding training
  * Partition data (data parallel)
  * Spawn 1 parameter actor (synchronization unit, singleton actor)
  * Spawn N worker actors per nodes
  * Spawn 1 worker per node to fetch data
  * After X minimatches workers synchronize with the parameter actor
* Try implementing an asynchronous implementation without the paramter actor
* Try implementing optimization strategies for reducing the overall network traffic (see [1])

### Evaluation

* Compare training duration and accurracy to baseline implementations
  * Accurracy: Research and use state of the art evaluation data sets (see [1] for data sets)
* Measure network load and cluster utilization
  * Node CPU/RAM usage
  * Inter-Node message exchange

### References

* [1] [Distributed Negative Sampling for Word Embeddings](https://www.aaai.org/ocs/index.php/AAAI/AAAI17/paper/viewFile/14956/14446)
* [2] [Network-Efficient Distributed Word2vec Training System for Large Vocabularies](https://arxiv.org/abs/1606.08495)
* [3] [Asynchronous Training of Word Embeddings for Large Text Corpora](https://arxiv.org/pdf/1812.03825.pdf)
* [4] [Parallelizing Word2Vec in Shared and Distributed Memory](https://arxiv.org/abs/1604.04661)
* [5] [Large Scale Distributed Deep Networks](http://papers.nips.cc/paper/4687-large-scale-distributed-deep-networks.pdf)
