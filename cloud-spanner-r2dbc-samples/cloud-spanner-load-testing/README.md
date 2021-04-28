

## Deploying application under test
1. Build app and create container image
    ````
   
   mvn -DskipTests package com.google.cloud.tools:jib-maven-plugin:3.0.0:build -Djib.from.image=registry://adoptopenjdk:11-jre -Dimage=gcr.io/[PROJECT_ID]/[IMAGE_NAME]

   # not anymore the following
    mvn -DskipTests package com.google.cloud.tools:jib-maven-plugin:build -Dimage=gcr.io/[PROJECT_ID]/[IMAGE_NAME]
    ````
   
   


## Creating GKE deployment
1. Replace `[PROJECT_ID]` AND `[IMAGE_NAME]` in `deployment.yaml` with the previously created container image name.


## Redeploying application under test

If image name changed:
````
kubectl set image deployment/r2dbc-load-testing r2dbc-load-testing=gcr.io/[PROJECT_ID]/[IMAGE_NAME]
````

Or, if there are custom credentials:
````
kubectl set image deployment/r2dbc-load-testing-with-sa r2dbc-load-testing=gcr.io/[PROJECT_ID]/[IMAGE_NAME]  --namespace [NAMESPACE]
````