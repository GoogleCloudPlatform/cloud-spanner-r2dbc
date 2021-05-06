## Creating GKE deployment (one time)
1. Replace `[PROJECT_ID]` AND `[IMAGE_NAME]` in `deployment.yaml` with the previously created container image name.

1. Draw the rest of the owl (TBD).


## Deploying application under test
1. Set variables for convenience.
   ````
   PROJECT_ID=[PROJECT_ID]
   APP_NAME=[APP_NAME]
   VERSION=[VERSION]
   NAMESPACE=[NAMESPACE] # for workload identity
   ````
1. Build app and create container image

    ````
   mvn -DskipTests package com.google.cloud.tools:jib-maven-plugin:3.0.0:build -Djib.from.image=registry://adoptopenjdk:11-jre -Dimage=gcr.io/$PROJECT_ID/$APP_NAME:$VERSION
    ````
   
   

## Redeploying application under test

If image name changed:

````
kubectl set image deployment/r2dbc-load-testing-with-sa r2dbc-load-testing=gcr.io/$PROJECT_ID/$APP_NAME:$VERSION --namespace $NAMESPACE
````


## Deploying to App Engine

While load testing environment is easier to control under GKE, profiling is easier in AppEngine 11, since the java agent is already pre-installed in the environment.

Keep in mind that AppEngine versions "May only contain lowercase letters, digits, and hyphens. Must begin and end with a letter or digit. Must not exceed 63 characters."

mvn package appengine:deploy -Dapp.deploy.projectId=$PROJECT_ID -Dapp.deploy.version=$VERSION

