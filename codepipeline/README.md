# Example Cross-Account CodeCommit/CodePipeline Stacks
The following Stacks will enable a Cross-Account CodePipeline deployment of the `examples/`.

## Setup
Create a Virtual Environment, activate it, and install the requirements:
```sh
python3 -m venv .env
source .env/bin/activate
pip install -r requirements.txt

```

## Deploy the Stacks
The Cross-Account configuration is:
- CodeCommit repository in Repo Account
- CodePipeline and Shared Resources in the Pipeline Account

1. Deploy the CodePipeline Shared Resources Stack in the Pipeline Account
   - This will create an S3 Bucket and KMS Key to be used for all CodePipelines
   - Update `codepipeline_shared_resources` with the Deployment and Trusted Account Ids
     - Update and redeploy anytime a new Trusted Account is added or removed 
   - Deploy the Stack
     ```bash
     cdk deploy -a ./codepipeline_shared_resources.py
     ```
 
 2. Note the Bucket and Key outputs
 
 3. Deploy the Cross-Account CodeCommit Resources Stack in the Repo Account
    - This will create and authorize a Cross-Account Role used for accessing CodeCommit
    - Update the `cross_account_codecommit_resources.py` with the Deployment and Trusted 
      Account Ids, Bucket, Key, and CodeCommit Repo 
      - Update and redeploy anytime a new Trusted Account is added or removed
    - Deploy the Stack
      ```bash
      cdk deploy -a ./cross_account_codecommit_resources.py
      ```
      
  4. Note the Role ARN output
  
  5. Deploy the Deployment Pipeline Stack
     - This will create and authorize a CodePipeline to deploy the `examples/`
     - Update the `deployment_pipeline` with the Deployment Account Id, Bucket, Key, 
       CodeCommit Repo, and Role
     - Deploy the Stack
       ```bash
       cdk deploy -a ./deployment_pipeline.py
       ``` 