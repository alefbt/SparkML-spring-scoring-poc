
![Head](https://github.com/alefbt/SparkML-spring-scoring-poc/blob/master/images/head.png "Header")

# SparkML-spring-scoring-poc
POC of socring rest service od Spark ML Pipelines.
The 

## Motivation
Serve Apache spark pipelines from Kubernetes

## Architecure
REST JSON Request -> K8s service -> Apache Spark

![Architecture](https://github.com/alefbt/SparkML-spring-scoring-poc/blob/master/images/arch1.png "Architecture 1")

# Getting started
in the code `data/pipline-archive` it pyspark pipeline 


## 1. Create serving pipeline
Create simple pipeline and Save pipeline 
```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.session import SparkSession

sc = SparkContext("local[2]", "SparkML")
spark = SparkSession(sc)

# Prepare training documents from a list of (id, text, label) tuples.
training = spark.createDataFrame([
    (0, "a b c d e spark", 1.0),
    (1, "b d", 0.0),
    (2, "spark f g h", 1.0),
    (3, "hadoop mapreduce", 0.0)
], ["id", "text", "label"])

# Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
lr = LogisticRegression(maxIter=10, regParam=0.001)
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

# Fit the pipeline to training documents.
model = pipeline.fit(training)

# This is the save pipeline method
model.save("/some-where-ml/project1/pipeline")

```

## 2. Adding `/some-where-ml/project1/mlserving.json` 
```json
{
    "name":"Test pipeline",
    "pipeline":"simple_pipline1",
    "schema":[
        {"name":"name","type":"STRING", "isNullable":false}
    ],
    "sample":{
        "name":"Bob"
    },
    "output": ["name", "features"]
}
```
## 3. Zip
No zip the folder `/some-where-ml/project1` and put it in your `pipelines.folder`
```bash
zip -r spark-sample-pipeline.zip /some-where-ml/project1 
```

## 4. Runing & Executing
in `application.properties` change the `pipelines.folder` to pipeline store folder -OR- run as paramete (like below)

```bash
# Build mvn
mvn install package

# Run
java -Dpipelines.folder=/some-where-ml -jar target/mlserver.jar
```

## 5. Call REST

```bash
curl -X POST \
	 -H "Content-Type: application/json" \
	 -d '[{"text":"Yehuda"}]' \
	 http://localhost:9900/predict/spark-sample-pipeline
```

![Runnig POC](https://github.com/alefbt/SparkML-spring-scoring-poc/blob/master/images/poc-serv1.png "Running POC")


## 6. (OPTIONAL) What next?
* add Warm-up for modules  
* dockerize and serve it as service

# Dockerization

To do some docker build
```bash
mvn clean install package && docker build  -t alefbt/spring-mlspark-serving .

```
Then run
```bash
docker run  --rm -it -p 9900:8080  alefbt/spring-mlspark-serving
```

# Notes
* This is code is **not optimized** to sub-second serving, it's possibol <,i did it on other project ;-) in order to do it, you need do some cacheing>
* Code contribution is welcome !
* Remember: this porject is POC.

# Known issues
* FIXED. Snappy (`xerial.snappy` package) doing some problems when running on vanilla Docker `openjdk:8-jdk-alpine` - see the `Dockerfile` explaines the walkaround


# Licence
MIT - Free.

> Use and Contribute
