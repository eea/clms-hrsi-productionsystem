
server_ip = '45.130.29.48'
#if server_port is different than server_client_port, you must activate redirection of server_client_port to server_port like so
#iptables -t nat -A PREROUTING -p tcp --dport $server_port_client -j REDIRECT --to-port $server_port
server_port = 65432
server_port_client = 80
adress_share_products = 'bar:reprocessing/system/l1c_exchange'
adress_share_products_external = 'distribute-external:product_exchange'
cosims_identifier = 'McM'
