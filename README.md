# Running with AWS Lambda
Create a zip with the following structure

- deploy.zip
    - package 1
    - ...
    - package n
    - lambda_function.py

Then just drag and drop there!

source: </br>
https://docs.aws.amazon.com/lambda/latest/dg/python-package.html#python-package-dependencies

WARNING: need a custom psycopg2 package to work in AWS lambda environment: </br> 
https://github.com/jkehler/awslambda-psycopg2
