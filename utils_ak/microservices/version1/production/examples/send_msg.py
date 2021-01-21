from utils_ak.microservices import ProductionMicroservice, run_listener_async

m = ProductionMicroservice()
m.publish_json('colletion', 'topic', {'msg': 'message'})