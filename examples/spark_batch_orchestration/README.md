
# Welcome to your CDK Python project!

Initial Configuration
    
    1. Update config.json as needed, including the S3 bucket name 
      
    2. Add your spark script to emr_orchestration/steps/
    
    3. Update or duplicate the pyspark_step in emr_orchestration/stack.py [Line 95] according to your script's needs

    4. Update Triggering Logic in the 'should_lambda_trigger_pipeline' function in emr_trigger/lambda_source/trigger.py
        **Currently set to start a batch for every 1 megabyte of data for demo purposes**

deploy.sh with build=true will create an ecr repository in your account, if it does not yet exist, and push your docker image to that repository
Then it will execute the CDK command to deploy all infrastructure referenced in app.py 
       
The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the .env
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .env
$ source .env/bin/activate
```

After the init process completes and the virtualenv is created, you can use the following
step to test deployment

To add additional dependencies, for example other CDK libraries, just add
them to your `requirements.txt` or `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful CDK commands

 * `bash deploy.sh ls`          list all stacks in the app
 * `bash deploy.sh synth`       emits the synthesized CloudFormation template
 * `bash deploy.sh deploy`      build and deploy this stack to your default AWS account/region
 * `bash deploy.sh diff`        compare deployed stack with current state
 * `bash deploy.sh docs`        open CDK documentation

 ## Run the demo

 Steps to run the Data Pipelines:
    1. Upload the parquet files stored under /sample_data folder in S3, into the "input" S3 Bucket as it follows:
        /demo-pipeline-s3inputbucket-srcbucketa467747d-unbfunfcy9dm/
            partition_folder/
                file_slot=1/
                    part-r-00....parquet
                    part-r-00....parquet
    2. Go to EMR console and check that a cluster is being created, including 2 steps
    3. When the 2 steps are completed, open the logs "stdout" to check results, and check for output data in "output" S3 bucket.