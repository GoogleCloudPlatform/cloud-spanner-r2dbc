from locust import HttpUser, task, between

class SpannerUser(HttpUser):
    
    @task
    def r2dbc_clientlibrary(self):
           self.client.get("/artworks/r2dbc-clientlibrary")

