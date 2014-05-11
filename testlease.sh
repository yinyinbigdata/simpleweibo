#!/bin/sh
# server -port 9009 -N 2 -id 2 &
# 
# # start slave
# server -port 9011 -master 127.0.0.1:9009 -id 1 &
# 
# sleep 4

client uc dga
client uc bryant
client uc imoraru

client sl dga
client sa dga bryant
client sl dga
client sa dga imoraru
client sl dga
client sl dga

# client tl dga
# client tp dga "First post"
# client tl dga
# client tp dga "Second post"
# client tl dga
#  
# 
# client sa bryant imoraru
# client sa bryant dga
# client tp imoraru "Iulian's first post"
# client tp imoraru "Iulian's second post"
# client ts bryant
