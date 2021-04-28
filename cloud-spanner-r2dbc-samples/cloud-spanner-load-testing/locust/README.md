This is roughly based on https://github.com/GoogleCloudPlatform/distributed-load-testing-using-kubernetes, but with updated versions of Python artifacts.


## Redeploying

1. Rebuild the image

    ````
    PROJECT=[PROJECT_ID]
    gcloud builds submit --tag gcr.io/$PROJECT/locust-tasks:latest docker-image
    ````

2. Restart pods to pull the new image
    ````
    kubectl rollout restart deploy locust-master  --namespace=customcreds
    kubectl rollout restart deploy locust-worker  --namespace=customcreds

    ````
   
   
3. Make sure the deployments are healthy from [Cloud console](https://console.cloud.google.com/kubernetes/workload), then port forward to local machine
   ````
   kubectl port-forward --namespace=customcreds service/locust-master 8089:8089
   ````
   
4. Go to http://localhost:8089/, and load test in peace.