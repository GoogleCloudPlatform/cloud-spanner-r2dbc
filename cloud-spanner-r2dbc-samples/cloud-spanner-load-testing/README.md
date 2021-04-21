

## Deploying application under test
1. Build app and create container image
    ````
    mvn -DskipTests package com.google.cloud.tools:jib-maven-plugin:build -Dimage=gcr.io/[PROJECT_ID]/[IMAGE_NAME]
    ````
   
   


## Creating GKE deployment
1. Replace `[PROJECT_ID]` AND `[IMAGE_NAME]` in `deployment.yaml` with the previously created container image name.


## Redeploying application under test

If image name changed:
````
kubectl set image deployment/r2dbc-load-testing r2dbc-load-testing=gcr.io/[PROJECT_ID]/[IMAGE_NAME]
````