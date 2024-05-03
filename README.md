[![Pharos](https://github.com/viralemergence/pharos-database/blob/prod/img/pharos-banner.png)](https://pharos.viralemergence.org/)

This repository is part of the [Pharos project](https://pharos.viralemergence.org/)
which is split into three repositories:

| Repository                                                                       | Purpose                                               |
| ---------------------------------------------------------------------------------| ------------------------------------------------------|
| [`pharos-frontend`](https://github.com/viralemergence/pharos-frontend)           | Frontend application and deployment infrastructure    |
| [`pharos-api`](https://github.com/viralemergence/pharos-api)                     | API and deployment infrastructure                     |
| [`pharos-database`](https://github.com/viralemergence/pharos-database)           | SQL database and deployment infrastructure            |
| [`pharos-documentation`](https://github.com/viralemergence/pharos-documentation) | Markdown files used to generate about pages |

<br>
<br>
<br>
<h1 align="center">
  Pharos API
</h1>

## 🚀 Deployment Status

Click the badges below to view more information about builds on that branch.
Changes pushed to any CI/CD branch will automatically be deployed to the
corresponding environment.

| Branch    | CI/CD Status                                                                                                                                                                                                                                                               | Url                                                                                                                          |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `prod`    | [![CircleCI](https://dl.circleci.com/status-badge/img/circleci/39PL8myokkHY7obZPJeFEC/Q3ya5vyUY8Lq4TTPcxM7Sz/tree/prod.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/circleci/39PL8myokkHY7obZPJeFEC/baac8a1b-cc90-4da0-b42c-9141f8340dab/tree/prod)       | [https://rt036ira4i.execute-api.us-west-1.amazonaws.com/Prod/](https://rt036ira4i.execute-api.us-west-1.amazonaws.com/Prod/) |
| `staging` | [![CircleCI](https://dl.circleci.com/status-badge/img/circleci/39PL8myokkHY7obZPJeFEC/Q3ya5vyUY8Lq4TTPcxM7Sz/tree/staging.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/circleci/39PL8myokkHY7obZPJeFEC/baac8a1b-cc90-4da0-b42c-9141f8340dab/tree/staging) | [https://x77ethnbni.execute-api.us-west-1.amazonaws.com/Prod/](https://x77ethnbni.execute-api.us-west-1.amazonaws.com/Prod/) |
| `review`  | [![CircleCI](https://dl.circleci.com/status-badge/img/circleci/39PL8myokkHY7obZPJeFEC/Q3ya5vyUY8Lq4TTPcxM7Sz/tree/review.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/circleci/39PL8myokkHY7obZPJeFEC/baac8a1b-cc90-4da0-b42c-9141f8340dab/tree/review)   | [https://wc85irdg5d.execute-api.us-west-1.amazonaws.com/Prod/](https://wc85irdg5d.execute-api.us-west-1.amazonaws.com/Prod/) |
| `dev`     | [![CircleCI](https://dl.circleci.com/status-badge/img/circleci/39PL8myokkHY7obZPJeFEC/Q3ya5vyUY8Lq4TTPcxM7Sz/tree/dev.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/circleci/39PL8myokkHY7obZPJeFEC/baac8a1b-cc90-4da0-b42c-9141f8340dab/tree/main)        | [https://9itsn2ewjb.execute-api.us-west-1.amazonaws.com/Prod/](https://9itsn2ewjb.execute-api.us-west-1.amazonaws.com/Prod/) |

## 👩‍💻 Development Quick start

1. Install [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
1. Configure AWS SSO using the [Pharos AWS Access Portal](https://viralemergence.awsapps.com/start/)
1. Install [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
1. Create a development stack using the following command:

```sh
sam sync --stack-name pharos-api-[DEVELOPER_NAME]-dev-rds --region us-west-1 --template-file ./template.yaml
```

This will output the API url, which can be passed to the pharos-frontend develop command.

## Run Database & Tests Locally

### 1. Start local testing database using Docker:

This command will start a docker container called `pharos-pytest-database` which
is intended to be a completely throwaway database (will not persist data on shutdown)
only for running local tests using pytest.

The database will be populated with records during the course of running tests as necessary.

```sh
docker run --rm \
    -P -p 127.0.0.1:5432:5432 \
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
pytest -v --cov=src/libraries/python/
```

This command also creates and displays a coverage report, the full
interactive coverage report can be viewed in a browser on port 8080
(chosing 8080 just because the frontend runs on 8000) with the command:

```sh
source env/bin/activate
pytest -v --cov=src/libraries/python/ --cov=src/lambda/ --cov-report=html
cd htmlcov; python -m http.server 8080
```

## 🏙️ Deployment Infrastructure

All infrastructure is managed in `template.yaml`, and all changes to that template
should be deployed by committing to a CCI tracking branch.

Pharos is a hybrid serverless application, so multiple deployments of `pharos-api`
will create multiple independent stacks with separate serverless resources, but
the deployments will share the database server and networking resources deployed
by the `pharos-database` stack. This means there is no marginal cost to deploying
additional instances of `pharos-api`, because these resources are billed only
according to usage.

In general, `pharos-api` consists of two api gateways, one for binary output for
map tiles, and one for JSON output, which serves all other API routes. Each API
route is served by a lambda function, with permissions to access stateful resources
(DynamoDB and S3) or the database server (deployed as
[`pharos-database`](https://github.com/viralemergence/pharos-database)) as necessary.

### Simplified Infrastructure Diagram

![Overview diagram](https://github.com/viralemergence/pharos-api/blob/dev/img/pharos-api-highlevel.png)
