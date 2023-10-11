## Build script for full, no-cache build;
## replace "verena-prod-dev" with correct profile name
sam build \
  --no-cached \
  --template-file ./template.yaml;
sam deploy \
  --config-env prod \
  --no-confirm-changeset \
  --no-fail-on-empty-changeset \
  --force-upload \
  --profile verena-prod-dev
