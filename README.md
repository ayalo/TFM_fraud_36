
# TFM_fraud_36

## Development of novel algorithms for fraud detection in online advertising
 
 This proyect is a proof of concept for detecting malicious webs in large datasets 
 based on the paper :** "Using Co-Visitation Networks For Detecting Large Scale Online Display Advertising Exchange Fraud"**
 
 The goal of the project is to implement new algorithms proposed in the literature, 
 including entropy-based methods,co-visitation patterns, etc.  and test the algorithms 
 in large datasets.
 Thanks to a set of logs, we will try to draw patterns of behavior between domains 
 considered malicious, based on the co-visitation degree  and  relationships  between 
 domains  with  similar  content  andcomparing them with domains considered lawful.
  
## Getting Started

Download the full code. 

The following instructions will let you execute the 3 main python classes described below locally for development and testing purposes. 

### Build with or Prerequisites

This proyect was developed in Python using Pycharm Build#PC-183.5912.18

Also this packages were installed: 

```
{
conda          4.6.12 
Python         3.6.8 
graphframes    0.6                    
pyspark        2.4.0                   
python-igraph  0.7.1.post7   
}
```

Be carefull with the environment variables :

|    variable           |    path                                                                                           | 
| ----------------------|:-------------------------------------------------------------------------------------------------:| 
| SPARK_HOME            | .../spark-2.4.1-bin-hadoop2.7/                                                                    | 
| PYTHONPATH            |  .../spark-2.4.1-bin-hadoop2.7/python; .../spark-2.4.1-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip|  
| PYSPARK_PYTHON        | .../anaconda3/envs/fraud_36/bin/python                                                            | 
| PYSPARK_DRIVER_PYTHON | .../anaconda3/envs/fraud_36/bin/python                                                            | 
| PYSPARK_SUBMIT_ARGS   | --packages graphframes:graphframes:0.7.0-spark2.4-s_2.11  pyspark-shell                           | 


Also for generating the folder structure, was used pybuilder and tree. 


### Code Organization 

The proyect structre looks like the following image : 

<img src="https://github.com/ayalo/TFM_fraud_36/blob/master/docs/images/tree_fraud36.png" width="350" height="350">

Three main clases into the **fraud36/source/** folder, allow us to execute the code to obtain, different graphs and plots. 

Into the folder **fraud36/notebooks**, there are 3 Jupyter Notebooks 

with some tests to different datasets and classes for the **'..._main'.py** previously cited.

Into the folder **fraud36/utils**, there are code distributed in classes to use in different data structures; for example : df_utils.py contains the functions that 
apply to an spark dataframe; gf_utils.py contains functions that apply to a graphframes graph; draw_utils.py function to plot and draw ... 

<img src="https://github.com/ayalo/TFM_fraud_36/blob/master/docs/images/utils_fraud36.png" width="200" height="100">

Also there is a zip : **'fraud36/source/utils.zip'** in order to make the use of this functions easy using a Jupyter Notebook. 