#!/bin/bash

# 创建工作目录
mkdir -p layer_build
cd layer_build

cat > requirements.txt << EOL
boto3
sqllineage
acryl-datahub
psycopg2-binary
EOL

#sudo dnf install -y postgresql-devel python3-devel gcc

python3.9 -m venv create_layer
source create_layer/bin/activate
pip install -r requirements.txt
mkdir python
cp -r create_layer/lib python/
zip -r layer_content.zip python
