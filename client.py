from main import Client


cl1 = Client('127.0.0.1:8011')

ip_address = "127.0.0.1"
port = 8011
text = "text_"

# n = 5
# for i in range(n):
#     text += str(i)
#     port += 1
#     cl1.M_API.add_node(f"{ip_address}:{port}", cl1.M_API.m_address_id)

d = cl1.M_API.get_stats()
print(d)