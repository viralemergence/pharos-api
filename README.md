<h1 align="center">
  Pharos API
</h1>

## 🚀 Deployment Status

| Branch  | CI/CD Status                                                                                                                                                                                                                                                             | Url                                                                                                                          |
| ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------- |
| Prod    | [![CircleCI](https://dl.circleci.com/status-badge/img/gh/talus-analytics-bus/pharos-api/tree/prod.svg?style=svg&circle-token=8821029cd3c99286dd5722e13653e78a12ff92b9)](https://dl.circleci.com/status-badge/redirect/gh/talus-analytics-bus/pharos-api/tree/prod)       | [https://rt036ira4i.execute-api.us-west-1.amazonaws.com/Prod/](https://rt036ira4i.execute-api.us-west-1.amazonaws.com/Prod/) |
| Staging | [![CircleCI](https://dl.circleci.com/status-badge/img/gh/talus-analytics-bus/pharos-api/tree/staging.svg?style=svg&circle-token=8821029cd3c99286dd5722e13653e78a12ff92b9)](https://dl.circleci.com/status-badge/redirect/gh/talus-analytics-bus/pharos-api/tree/staging) | [https://x77ethnbni.execute-api.us-west-1.amazonaws.com/Prod/](https://x77ethnbni.execute-api.us-west-1.amazonaws.com/Prod/) |
| Review  | [![CircleCI](https://dl.circleci.com/status-badge/img/gh/talus-analytics-bus/pharos-api/tree/review.svg?style=svg&circle-token=8821029cd3c99286dd5722e13653e78a12ff92b9)](https://dl.circleci.com/status-badge/redirect/gh/talus-analytics-bus/pharos-api/tree/review)   | [https://wc85irdg5d.execute-api.us-west-1.amazonaws.com/Prod/](https://wc85irdg5d.execute-api.us-west-1.amazonaws.com/Prod/) |
| Dev     | [![CircleCI](https://dl.circleci.com/status-badge/img/gh/talus-analytics-bus/pharos-api/tree/dev.svg?style=svg&circle-token=8821029cd3c99286dd5722e13653e78a12ff92b9)](https://dl.circleci.com/status-badge/redirect/gh/talus-analytics-bus/pharos-api/tree/dev)         | [https://9itsn2ewjb.execute-api.us-west-1.amazonaws.com/Prod/](https://9itsn2ewjb.execute-api.us-west-1.amazonaws.com/Prod/) |

This project contains source code and supporting files for a serverless application that you can deploy with the SAM CLI. It includes the following files and folders.

## 👩‍💻 Development Quick start

1. Install [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
2. Install [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
3. Create a development stack using the following command:

```sh
sam sync --stack-name pharos-api-[DEVELOPER_NAME]-dev-rds --region us-west-1 --template-file ./template.yaml
```

This will output the API url, which can be passed to the pharos-frontend develop command.

## 🖥 Deployment Infrastructure

All infrastructure is managed in `template.yaml`, and all changes to that template
should be deployed by committing to a CCI tracking branch.

## Run Tests Locally

### 1. Start local testing database using Docker:

This command will start a docker container called `pharos-pytest-database` which
is intended to be a completely throwaway database (will not persist data on shutdown)
only for running local tests using pytest.

```sh
docker run --rm -P -p 127.0.0.1:5432:5432 \
    -e POSTGRES_PASSWORD="1234" \
    -e POSTGRES_DB="pharos-pytest" \
    --name pharos-pytest-database \
    postgis/postgis
```

### 2. Install and source dev dependencies

```sh
python3.9 -m venv env .
source env/bin/activate
pip install -r dev-requirements.txt
```

### 3. Run project tests

```
source env/bin/activate
pytest -v --cov=src/libraries/python/ --cov=src/lambda/
```

This command also creates and displays a coverage report, the
full interactive coverage report can be viewed in a browser
on port 8080 (chosing 8080 just because the frontend runs on 8000) with the command:

```sh
cd htmlcov; python -m http.server 8080
```
