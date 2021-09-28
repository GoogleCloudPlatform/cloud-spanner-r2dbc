from locust import HttpUser, task, between

class SpannerUser(HttpUser):

    @task
    def inventory_load_test(self):
           self.client.post("/delete")

