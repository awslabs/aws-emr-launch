# Example Cross-Account CodeCommit/CodePipeline Stacks
The following Stacks will enable a Cross-Account CodePipeline deployment of the `examples/`.

## Setup
Create a Virtual Environment, activate it, and install the requirements:
```bash
python3 -m venv .env
source .env/bin/activate
pip install -r requirements.txt

```

## Deploy the Stacks
```bash
cdk deploy -a ./examples_pipeline.py
cdk deploy -a ./release_pipeline.py
```
