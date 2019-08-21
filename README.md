
# TFM_fraud_36

## Development of novel algorithms for fraud detection in online advertising
 
 This proyect is a proof of concept for detecting malicious webs in large datasets 
 based on the paper : "Using Co-Visitation Networks For Detecting Large Scale Online Display Advertising Exchange Fraud"
 
 The goal of the project is to implement new algorithms proposed in the literature, 
 including entropy-based methods,co-visitation patterns, etc.  and test the algorithms 
 in large datasets.
 Thanks to a set of logs, we will try to draw patterns of behavior between domains 
 considered malicious, based on the co-visitation degree  and  relationships  between 
  domains  with  similar  content  andcomparing them with domains considered lawful.
  
## Getting Started

Download the full code. 

The following instructions will let you execute the python classes described below locally for development and testing purposes. 

### Prerequisites

This proyect was developed in Python using Pycharm Build#PC-183.5912.18

Also this packages were installed: 

conda         4.6.12
Python            3.6.8
graphframes    0.6                    
pyspark        2.4.0                  
python-igraph  0.7.1.post7  

Be carefull with the environment variables :

SPARK_HOME  = .../spark-2.4.1-bin-hadoop2.7/
PYTHONPATH  = .../spark-2.4.1-bin-hadoop2.7/python;.../spark-2.4.1-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip
PYSPARK_PYTHON = .../anaconda3/envs/fraud_36/bin/python
PYSPARK_DRIVER_PYTHON = .../anaconda3/envs/fraud_36/bin/python
PYSPARK_SUBMIT_ARGS = --packages graphframes:graphframes:0.7.0-spark2.4-s_2.11  pyspark-shell

Also for generating the folder structure, was used pybuilder and tree. 


### Installing

The proyect structre looks like the following image : 
![alt text](https://github.com/ayalo/TFM_fraud_36/edit/master/docs/images/tree_fraud36.png.png)

