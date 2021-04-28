from locust import HttpUser, task, between

class SpannerUser(HttpUser):
    
    @task
    def r2dbc_clientlibrary(self):
           self.client.get("/artworks/r2dbc-clientlibrary")

    @task
    def r2dbc_grpc(self):
           self.client.get("/artworks/r2dbc-grpc")

    @task
    def clientlibrary_all(self):
           self.client.get("/artworks/client-library-all")

    @task
    def clientlibrary_reactive(self):
           self.client.get("/artworks/client-library-reactive")
