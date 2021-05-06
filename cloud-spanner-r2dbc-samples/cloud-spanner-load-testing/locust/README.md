This image is roughly based on https://github.com/GoogleCloudPlatform/distributed-load-testing-using-kubernetes, but with updated versions of Python artifacts.

It also creates tagged versions of the image, so it's easy to switch between different Locust task sets.

## Redeploying

1. Rebuild the image, unless an image with this tag already exists in the registry.

    ````
    PROJECT=[PROJECT_ID]
    VERSION=[VERSION]   # one of r2dbc-grpc, r2dbc-clientlibrary etc.
    gcloud builds submit --tag gcr.io/$PROJECT/locust-tasks:$VERSION docker-image
    ````

2. Update the workload image:

   ````
   kubectl set image --namespace=customcreds deployment/locust-master locust-master=gcr.io/$PROJECT/locust-tasks:$VERSION
   kubectl set image --namespace=customcreds deployment/locust-worker locust-worker=gcr.io/$PROJECT/locust-tasks:$VERSION
   ````

   
   
3. Make sure the deployments are healthy from [Cloud console](https://console.cloud.google.com/kubernetes/workload), then port forward to local machine
   ````
   kubectl port-forward --namespace=customcreds service/locust-master 8089:8089
   ````
   
4. Go to http://localhost:8089/, and load test in peace.