from main import Client


cl1 = Client('127.0.0.1:8011')

ip_address = "127.0.0.1"
port = 8011
text = "text_"

d = cl1.M_API.get_stats()
print(d)

cl1._update_stats()
print(cl1.R_API)
print(cl1.W_API)

#does not work and cause an error
cl1.get_message()
cl1.push_message("test")




#------------THIS CODE IS NOT NECESSARY TO BE COMPILED. IT CAN BE RUN ONLY ONE TIME--------------------------
#------------THIS PART OF CODE ADD SOME DATA TO THE DATABASE-------------------------------------------------

# n = 5
# for i in range(n):
#     text += str(i)
#     port += 1
#     cl1.M_API.add_node(f"{ip_address}:{port}", cl1.M_API.m_address_id)

# for j in range(125):
#     cursor.execute(
#         "INSERT INTO messages (message, fk_d_address) VALUES(%s, %s)", (f"{text}{j}", 1)
#     )
#
# for j in range(156):
#     cursor.execute(
#         "INSERT INTO messages (message, fk_d_address) VALUES(%s, %s)", (f"{text}{j}", 2)
#     )
#
# for j in range(140):
#     cursor.execute(
#         "INSERT INTO messages (message, fk_d_address) VALUES(%s, %s)", (f"{text}{j}", 3)
#     )
#
# for j in range(190):
#     cursor.execute(
#         "INSERT INTO messages (message, fk_d_address) VALUES(%s, %s)", (f"{text}{j}", 4)
#     )
#
# for j in range(110):
#     cursor.execute(
#         "INSERT INTO messages (message, fk_d_address) VALUES(%s, %s)", (f"{text}{j}", 5)
#     )

