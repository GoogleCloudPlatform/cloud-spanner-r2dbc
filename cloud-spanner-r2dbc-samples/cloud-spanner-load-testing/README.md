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


## Profiling on GKE

NOTE: make sure your service has the Cloudprofiler Agent role; otherwise no data will be reported.

The easiest way to enable java agent on GKE is to include it as an extra file in the src/main/jib folder.
1. Install cprof into the folder within this application:
````
mkdir -p src/main/jib/opt/cprof

wget -q -O- https://storage.googleapis.com/cloud-profiler/java/latest/profiler_java_agent.tar.gz | tar xzv -C src/main/jib/opt/cprof

```` 

1. And now create a container that starts java with the agent enabled.
````
mvn -DskipTests package com.google.cloud.tools:jib-maven-plugin:3.0.0:build -Djib.from.image=registry://adoptopenjdk:11-jre -Dimage=gcr.io/$PROJECT_ID/$APP_NAME:$VERSION -Djib.container.jvmFlags="-agentpath:/opt/cprof/profiler_java_agent.so=-cprof_service=r2dbc-profiling"
````