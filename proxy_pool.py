"""
# Project: proxies pool
# Author: Eddie
# Date:  22/06/2023
"""
# Clear the proxy_text
proxy_text = 'proxy_text.txt'

with open(proxy_text, 'w') as file:
    file.write('')


def proxy_generation(number):
    for i in range(number):
        ###########
        proxyip = "http://storm-stst123_area-PL:123123@proxy.stormip.cn:1000"
        url = "http://myip.ipip.net"
        proxies = {
            'http': proxyip,
            'https': proxyip,
        }
        print(proxies)
        with open(proxy_text, 'a') as file:
            file.write(proxyip)
            file.write('\n')

        print("Data saved to", proxy_text)

# Change the number to decide the number of proxies generated
proxy_generation(5)

