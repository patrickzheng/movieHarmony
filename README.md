# moviEharmony
[www.moviEharmony.com](http://movieharmony.com)

[Slideshow](https://docs.google.com/presentation/d/1-A-qJugSwMEH5jcRqVluHKBr8kAQCnNkOQnQCe6Hrvo/edit?usp=sharing)

moviEharmony.com is a data platform which can finds movies
which 2 people may like to watch together.
It is completely open-source and uses the following technologies:

- Apache Kafka
- Python 
- Amazon S3
- Spark / Spark MLlib
- Apache Cassandra
- Flask

## The moviEharmony Website

![](https://github.com/patrickzheng/movieHarmony/ref/s1.png?raw=true)

moviEharmony.com is currently batch processing (as of Oct 7, 2015) Amazon review dataset. These reviews provide the data which drive the following components of moviEharmony.com:

- _MovieSearch_: Allows 2 users to find movies that they may like to watch together.

![](https://github.com/patrickzheng/movieHarmony/ref/s2.png?raw=true)

- _QueryUser_: Allows users to find what they have reviewed in the past

![](https://github.com/patrickzheng/movieHarmony/ref/s3.png?raw=true)

- _EnterReview_: Allows users to add movie reviews

![](https://github.com/patrickzheng/movieHarmony/ref/s3.png?raw=true)
