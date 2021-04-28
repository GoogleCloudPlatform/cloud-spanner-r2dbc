from locust import HttpUser, task, between

class SpannerUser(HttpUser):

    @task
    def r2dbc_grpc(self):
        self.client.get("/artworks/noop-1s-delay")

