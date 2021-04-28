from locust import HttpUser, task, between

class SpannerUser(HttpUser):
    
    @task
    def clientlibrary_all(self):
           self.client.get("/artworks/client-library-all")

