from locust import HttpUser, task, between

class SpannerUser(HttpUser):

    @task
    def clientlibrary_reactive(self):
           self.client.get("/artworks/client-library-reactive")

