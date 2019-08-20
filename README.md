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

conda 		    4.6.12
Python 		    3.6.8
graphframes   	0.6                    
pyspark       	2.4.0                  
python-igraph 	0.7.1.post7  

Be carefull with the environment variables :

SPARK_HOME  = .../spark-2.4.1-bin-hadoop2.7/
PYTHONPATH  = .../spark-2.4.1-bin-hadoop2.7/python;.../spark-2.4.1-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip
PYSPARK_PYTHON = .../anaconda3/envs/fraud_36/bin/python
PYSPARK_DRIVER_PYTHON = .../anaconda3/envs/fraud_36/bin/python
PYSPARK_SUBMIT_ARGS = --packages graphframes:graphframes:0.7.0-spark2.4-s_2.11  pyspark-shell

Also for generating the folder structure, was used pybuilder and tree. 


### Installing

The proyect structre looks like the following image : 
![alt text](https://github.com/ayalo/TFM_fraud_36/edit/master/path/to/img.png)


A step by step series of examples that tell you how to get a development env running

Say what the step will be

```
Give the example
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo

## Running the tests

Explain how to run the automated tests for this system

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment





## Built With

* [Dropwizard](http://www.dropwizard.io/1.0.2/docs/) - The web framework used
* [Maven](https://maven.apache.org/) - Dependency Management
* [ROME](https://rometools.github.io/rome/) - Used to generate RSS Feeds

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 

## Authors

* **Billie Thompson** - *Initial work* - [PurpleBooth](https://github.com/PurpleBooth)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc

